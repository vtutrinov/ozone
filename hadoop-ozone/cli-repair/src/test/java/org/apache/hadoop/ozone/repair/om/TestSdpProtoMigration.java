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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * Runs {@link SdpProtoMigration#migrate} on a RocksDB holding SDP-1.4 and upstream encoded keys.
 */
public class TestSdpProtoMigration {

  private static byte[] sdpKey(String name, String owner) {
    byte[] base = KeyInfo.newBuilder().setVolumeName("vol").setBucketName("bucket").setKeyName(name)
        .setDataSize(1).setType(HddsProtos.ReplicationType.RATIS).setCreationTime(1).setModificationTime(1)
        .build().toByteArray();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(base, 0, base.length);
    byte[] ownerBytes = owner.getBytes(StandardCharsets.UTF_8);
    SdpProtoRewriter.writeVarint(out, SdpProtoRewriter.tag(22, SdpProtoRewriter.LEN));
    SdpProtoRewriter.writeVarint(out, ownerBytes.length);
    out.write(ownerBytes, 0, ownerBytes.length);
    return out.toByteArray();
  }

  @Test
  void migratesKeyTableInPlace(@TempDir Path dir) throws Exception {
    List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("metaTable".getBytes(StandardCharsets.UTF_8)),
        new ColumnFamilyDescriptor("keyTable".getBytes(StandardCharsets.UTF_8)));
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (ManagedDBOptions options = new ManagedDBOptions()) {
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);
      try (ManagedRocksDB db = ManagedRocksDB.open(options, dir.toString(), descriptors, handles)) {
        ColumnFamilyHandle keyTable = handles.get(2);
        for (int i = 0; i < 25_000; i++) {
          db.get().put(keyTable, ("/vol/bucket/k" + i).getBytes(StandardCharsets.UTF_8), sdpKey("k" + i, "u" + i));
        }
        byte[] upstream = KeyInfo.newBuilder().setVolumeName("vol").setBucketName("bucket").setKeyName("up")
            .setDataSize(1).setType(HddsProtos.ReplicationType.RATIS).setCreationTime(1).setModificationTime(1)
            .setOwnerName("bob").setExpectedDataGeneration(1).build().toByteArray();
        db.get().put(keyTable, "/vol/bucket/up".getBytes(StandardCharsets.UTF_8), upstream);

        // dry run: nothing written
        SdpProtoRewriter.Stats dryRun = SdpProtoMigration.migrate(db, handles, false);
        assertEquals(25_000, dryRun.get("KeyInfo.migrated"));
        assertFalse(KeyInfo.parseFrom(db.get().get(keyTable, "/vol/bucket/k7".getBytes(StandardCharsets.UTF_8)))
            .hasOwnerName());

        SdpProtoRewriter.Stats stats = SdpProtoMigration.migrate(db, handles, true);
        assertEquals(25_000, stats.get("KeyInfo.migrated"));
        assertEquals(1, stats.get("KeyInfo.skipped"));
        assertEquals("u7", KeyInfo.parseFrom(
            db.get().get(keyTable, "/vol/bucket/k7".getBytes(StandardCharsets.UTF_8))).getOwnerName());
        assertEquals("bob", KeyInfo.parseFrom(
            db.get().get(keyTable, "/vol/bucket/up".getBytes(StandardCharsets.UTF_8))).getOwnerName());
      } finally {
        handles.forEach(ColumnFamilyHandle::close);
      }
    }
  }
}
