/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_COMPRESSION_FILE_EXT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.CompressionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOzoneCompression {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;

  private static final int BLOCK_SIZE = 64 * 1024; // 64KB
  private static final int CHUNK_SIZE = 16 * 1024; // 16KB

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_COMPRESSION_FILE_EXT_KEY, "");
    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .applyTo(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
  }

  /** The OM configuration is where the compressed file extensions are read from. */
  private static OzoneConfiguration omConf() {
    return cluster.getOzoneManager().getConfiguration();
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("compressedExtensions")
  void testCompressOnlyAllowedExtensionsDefaults(String fileExt, boolean shouldCompress) throws IOException {
    omConf().unset(OZONE_COMPRESSION_FILE_EXT_KEY);

    OzoneBucket bucket = prepareBucket(BucketLayout.FILE_SYSTEM_OPTIMIZED, CompressionType.GZIP);

    validateExpectedCompression(fileExt, shouldCompress, bucket);
  }

  @Test
  void testCompressOnlyAllowedExtensionsCustom() throws IOException {
    omConf().set(OZONE_COMPRESSION_FILE_EXT_KEY, ".doc");

    OzoneBucket bucket = prepareBucket(BucketLayout.FILE_SYSTEM_OPTIMIZED, CompressionType.GZIP);

    validateExpectedCompression(".doc", true, bucket);
    validateExpectedCompression(".txt", false, bucket);
  }

  private static void validateExpectedCompression(String fileExt, boolean shouldCompress, OzoneBucket bucket)
      throws IOException {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID() + fileExt;
    String value = "sample value";
    int originalSize = value.getBytes(StandardCharsets.UTF_8).length;
    try (OzoneOutputStream out = bucket.createKey(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }

    // Verify content.
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));

    if (shouldCompress) {
      assertEquals(CompressionType.GZIP.getCodecName(), key.getCompressionType());
      assertEquals(originalSize, key.getOriginalDataSize());
    } else {
      assertTrue(StringUtils.isEmpty(key.getCompressionType()));
    }
  }

  public static Stream<Arguments> compressedExtensions() {
    return Stream.of(
        arguments(".txt", true),
        arguments(".log", true),
        arguments("log", false),
        arguments(".csv", true),
        arguments(".json", true),
        arguments(".tar", true),
        arguments(".xml", true),
        arguments(".bin", true),
        arguments(".doc", false)
    );
  }

  @ParameterizedTest
  @MethodSource("bucketArgs")
  void testPutKeyWithEncryption(BucketLayout bucketLayout, CompressionType compressionType) throws Exception {
    omConf().set(OZONE_COMPRESSION_FILE_EXT_KEY, "");

    OzoneBucket bucket = prepareBucket(bucketLayout, compressionType);

    createAndVerifyKeyData(bucket, compressionType);
    createAndVerifyFileData(bucket, compressionType);
    createAndVerifyStreamKeyData(bucket, compressionType);
  }

  private static OzoneBucket prepareBucket(
      BucketLayout bucketLayout,
      CompressionType compressionType
  ) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout)
        .setCompressionType(compressionType.getCodecName()).build();
    volume.createBucket(bucketName, bucketArgs);
    return volume.getBucket(bucketName);
  }

  private static Stream<Arguments> bucketArgs() {
    return Arrays.stream(BucketLayout.values()).
        flatMap(TestOzoneCompression::forLayout);
  }

  private static Stream<Arguments> forLayout(BucketLayout bucketLayout) {
    return Arrays.stream(CompressionType.values()).
        map(compressionType -> Arguments.of(bucketLayout, compressionType));
  }

  static void createAndVerifyKeyData(OzoneBucket bucket, CompressionType compressionType) throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    int originalSize = value.getBytes(StandardCharsets.UTF_8).length;
    try (OzoneOutputStream out = bucket.createKey(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime, originalSize);
    OzoneKeyDetails key1 = bucket.getKey(keyName);

    // Overwrite the key
    try (OzoneOutputStream out = bucket.createKey(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    OzoneKeyDetails key2 = bucket.getKey(keyName);
    assertEquals(key1.getCompressionType(), key2.getCompressionType());
    assertEquals(key1.getOriginalDataSize(), key2.getOriginalDataSize());
  }

  static void createAndVerifyFileData(OzoneBucket bucket, CompressionType compressionType) throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    int originalSize = value.getBytes(StandardCharsets.UTF_8).length;
    try (OzoneOutputStream out = bucket.createFile(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE), true, false)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime, originalSize);
    OzoneKeyDetails key1 = bucket.getKey(keyName);

    // Overwrite the key
    try (OzoneOutputStream out = bucket.createFile(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE), true, false)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    OzoneKeyDetails key2 = bucket.getKey(keyName);
    assertEquals(key1.getCompressionType(), key2.getCompressionType());
    assertEquals(key1.getOriginalDataSize(), key2.getOriginalDataSize());
  }

  static void createAndVerifyStreamKeyData(OzoneBucket bucket, CompressionType compressionType)
      throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    int originalSize = value.getBytes(StandardCharsets.UTF_8).length;
    try (OzoneDataStreamOutput out = bucket.createStreamKey(keyName,
        originalSize,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime, originalSize);
  }

  static void verifyKeyData(OzoneBucket bucket, String keyName, CompressionType compressionType,
                            Instant testStartTime, int originalSize) throws Exception {
    // Verify content.
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));

    assertEquals(compressionType.getCodecName(), key.getCompressionType());
    assertEquals(originalSize, key.getOriginalDataSize());
  }

}
