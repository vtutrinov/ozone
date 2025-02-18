package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.CompressionType;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestOzoneCompression {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static File testDir;
  private static OzoneConfiguration conf;

  private static final int BLOCK_SIZE = 64 * 1024; // 64KB
  private static final int CHUNK_SIZE = 16 * 1024; // 16KB

  @BeforeAll
  static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestSecureOzoneRpcClient.class.getSimpleName());

    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    CertificateClientTestImpl certificateClientTest =
        new CertificateClientTestImpl(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .setCertificateClient(certificateClientTest)
        .setSecretKeyClient(new SecretKeyTestClient())
        .build();
    cluster.getOzoneManager().startSecretManager();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
    TestOzoneRpcClient.setCluster(cluster);
    TestOzoneRpcClient.setOzClient(ozClient);
    TestOzoneRpcClient.setOzoneManager(ozoneManager);
    TestOzoneRpcClient.setStorageContainerLocationClient(
        storageContainerLocationClient);
    TestOzoneRpcClient.setStore(store);
    TestOzoneRpcClient.setClusterId(CLUSTER_ID);
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("bucketArgs")
  void testPutKeyWithEncryption(BucketLayout bucketLayout, CompressionType compressionType) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout)
        .setCompressionType(compressionType.getCodecName()).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    createAndVerifyKeyData(bucket, compressionType);
    createAndVerifyFileData(bucket, compressionType);
    createAndVerifyStreamKeyData(bucket, compressionType);
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
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime);
    OzoneKeyDetails key1 = bucket.getKey(keyName);

    // Overwrite the key
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    OzoneKeyDetails key2 = bucket.getKey(keyName);
    assertEquals(key1.getCompressionType(), key2.getCompressionType());
  }

  static void createAndVerifyFileData(OzoneBucket bucket, CompressionType compressionType) throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    try (OzoneOutputStream out = bucket.createFile(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE), true, false)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime);
    OzoneKeyDetails key1 = bucket.getKey(keyName);

    // Overwrite the key
    try (OzoneOutputStream out = bucket.createFile(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE), true, false)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    OzoneKeyDetails key2 = bucket.getKey(keyName);
    assertEquals(key1.getCompressionType(), key2.getCompressionType());
  }

  static void createAndVerifyStreamKeyData(OzoneBucket bucket, CompressionType compressionType)
      throws Exception {
    Instant testStartTime = Instant.now();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    try (OzoneDataStreamOutput out = bucket.createStreamKey(keyName,
        value.getBytes(StandardCharsets.UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        new HashMap<>())) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
    verifyKeyData(bucket, keyName, compressionType, testStartTime);
  }

  static void verifyKeyData(OzoneBucket bucket, String keyName, CompressionType compressionType,
                            Instant testStartTime) throws Exception {
    // Verify content.
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    assertFalse(key.getCreationTime().isBefore(testStartTime));
    assertFalse(key.getModificationTime().isBefore(testStartTime));

    assertEquals(compressionType.getCodecName(), key.getCompressionType());
  }

}
