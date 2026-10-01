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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.repair.om.SdpProtoRewriter.LEN;
import static org.apache.hadoop.ozone.repair.om.SdpProtoRewriter.VARINT;
import static org.apache.hadoop.ozone.repair.om.SdpProtoRewriter.tag;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedKeyInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SdpProtoRewriter}: SDP-1.4 encoded OM values are renumbered, upstream 2.x values are left alone.
 */
public class TestSdpProtoRewriter {

  private static KeyInfo.Builder baseKey(String name) {
    return KeyInfo.newBuilder().setVolumeName("vol").setBucketName("bucket").setKeyName(name)
        .setDataSize(100).setType(HddsProtos.ReplicationType.RATIS).setCreationTime(1).setModificationTime(2);
  }

  private static BucketInfo.Builder baseBucket() {
    return BucketInfo.newBuilder().setVolumeName("vol").setBucketName("bucket")
        .setIsVersionEnabled(false).setStorageType(HddsProtos.StorageTypeProto.DISK);
  }

  /** Appends fields encoded with the SDP-1.4 numbers. */
  private static byte[] withSdpFields(byte[] message, Object... fieldsAndValues) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(message, 0, message.length);
    for (int i = 0; i < fieldsAndValues.length; i += 2) {
      int field = (Integer) fieldsAndValues[i];
      Object value = fieldsAndValues[i + 1];
      if (value instanceof Long) {
        SdpProtoRewriter.writeVarint(out, tag(field, VARINT));
        SdpProtoRewriter.writeVarint(out, (Long) value);
      } else {
        byte[] bytes = value instanceof byte[] ? (byte[]) value : ((String) value).getBytes(StandardCharsets.UTF_8);
        SdpProtoRewriter.writeVarint(out, tag(field, LEN));
        SdpProtoRewriter.writeVarint(out, bytes.length);
        out.write(bytes, 0, bytes.length);
      }
    }
    return out.toByteArray();
  }

  private static byte[] sdpKey(String name) {
    return withSdpFields(baseKey(name).build().toByteArray(), 20, "gzip", 21, 12345L, 22, "alice");
  }

  @Test
  void sdpKeyInfoIsRenumbered() throws Exception {
    SdpProtoRewriter.Stats stats = new SdpProtoRewriter.Stats();
    byte[] migrated = SdpProtoRewriter.KEY_INFO.rewrite(sdpKey("k1"), stats);
    assertNotNull(migrated);
    KeyInfo key = KeyInfo.parseFrom(migrated);
    assertEquals("gzip", key.getCompressionType());
    assertEquals(12345L, key.getOriginalDataSize());
    assertEquals("alice", key.getOwnerName());
    assertEquals(0, key.getTagsCount());
    assertFalse(key.hasExpectedDataGeneration());
    assertEquals("k1", key.getKeyName());
    assertEquals(1, stats.get("KeyInfo.migrated"));
    assertEquals(1L, stats.getMovedCompressionValues().get("gzip"));
  }

  @Test
  void sdpKeyWithoutOptionalFields() throws Exception {
    // a key without compression / owner, only originalDataSize
    byte[] value = withSdpFields(baseKey("k2").build().toByteArray(), 21, 7L);
    KeyInfo key = KeyInfo.parseFrom(SdpProtoRewriter.KEY_INFO.rewrite(value, new SdpProtoRewriter.Stats()));
    assertEquals(7L, key.getOriginalDataSize());
    assertFalse(key.hasOwnerName());
    assertFalse(key.hasCompressionType());
  }

  @Test
  void upstreamAndMigratedKeysAreUnchanged() throws Exception {
    SdpProtoRewriter.Stats stats = new SdpProtoRewriter.Stats();
    // written by 2.x: ownerName 20 + tags 21 (length-delimited) + expectedDataGeneration 22 (varint)
    byte[] upstream = baseKey("k3").setOwnerName("bob").setExpectedDataGeneration(3)
        .addTags(HddsProtos.KeyValue.newBuilder().setKey("t").setValue("v")).build().toByteArray();
    assertNull(SdpProtoRewriter.KEY_INFO.rewrite(upstream, stats));
    // already migrated
    byte[] migrated = SdpProtoRewriter.KEY_INFO.rewrite(sdpKey("k4"), stats);
    assertNull(SdpProtoRewriter.KEY_INFO.rewrite(migrated, stats));
    // plain key without any SDP or contested field
    assertNull(SdpProtoRewriter.KEY_INFO.rewrite(baseKey("k5").build().toByteArray(), stats));
    assertEquals(2, stats.get("KeyInfo.skipped"));
  }

  @Test
  void nestedKeyInfosAreRenumbered() throws Exception {
    byte[] repeated = withSdpFields(RepeatedKeyInfo.newBuilder().setBucketId(9).build().toByteArray(),
        // d2 was written by 2.x: KeyInfo field 20 alone is ambiguous (the DB-level run-once marker covers it),
        // expectedDataGeneration (22, varint) identifies the 2.x record
        1, sdpKey("d1"), 1, baseKey("d2").setOwnerName("bob").setExpectedDataGeneration(1).build().toByteArray());
    RepeatedKeyInfo deleted = RepeatedKeyInfo.parseFrom(
        SdpProtoRewriter.REPEATED_KEY_INFO.rewrite(repeated, new SdpProtoRewriter.Stats()));
    assertEquals(2, deleted.getKeyInfoCount());
    assertEquals("alice", deleted.getKeyInfo(0).getOwnerName());
    assertEquals("gzip", deleted.getKeyInfo(0).getCompressionType());
    assertEquals("bob", deleted.getKeyInfo(1).getOwnerName());
    assertEquals(9, deleted.getBucketId());

    // PartKeyInfo with an SDP encoded KeyInfo
    byte[] sdpPart = withSdpFields(new byte[0], 1, "p", 2, 1L, 3, sdpKey("part"));
    byte[] multipart = withSdpFields(MultipartKeyInfo.newBuilder().setUploadID("u").setCreationTime(1)
        .setType(HddsProtos.ReplicationType.RATIS).build().toByteArray(), 5, sdpPart);
    MultipartKeyInfo mpu = MultipartKeyInfo.parseFrom(
        SdpProtoRewriter.MULTIPART_KEY_INFO.rewrite(multipart, new SdpProtoRewriter.Stats()));
    assertEquals("alice", mpu.getPartKeyInfoList(0).getPartKeyInfo().getOwnerName());
    assertEquals(12345L, mpu.getPartKeyInfoList(0).getPartKeyInfo().getOriginalDataSize());
  }

  @Test
  void bucketInfoIsRenumbered() throws Exception {
    UUID group = UUID.randomUUID();
    byte[] raftGroup = HddsProtos.UUID.newBuilder().setMostSigBits(group.getMostSignificantBits())
        .setLeastSigBits(group.getLeastSignificantBits()).build().toByteArray();
    byte[] sdp = withSdpFields(baseBucket().build().toByteArray(), 21, "snappy", 22, raftGroup);
    BucketInfo bucket = BucketInfo.parseFrom(SdpProtoRewriter.BUCKET_INFO.rewrite(sdp, new SdpProtoRewriter.Stats()));
    assertEquals("snappy", bucket.getCompressionType());
    assertEquals(group.getMostSignificantBits(), bucket.getRaftGroup().getMostSigBits());
    assertFalse(bucket.hasSnapshotUsedBytes());

    byte[] upstream = baseBucket().setSnapshotUsedBytes(5).setSnapshotUsedNamespace(6).build().toByteArray();
    assertNull(SdpProtoRewriter.BUCKET_INFO.rewrite(upstream, new SdpProtoRewriter.Stats()));
    assertArrayEquals(upstream, baseBucket().setSnapshotUsedBytes(5).setSnapshotUsedNamespace(6).build()
        .toByteArray());
  }
}
