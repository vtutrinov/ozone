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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Offline migration of an OM DB written by SDP Ozone 1.4 to the proto numbering of the SDP Ozone 2.x line, where
 * every SDP field moved to the 1000+ range (upstream reuses the old numbers):
 * <ul>
 *   <li>KeyInfo: compressionType 20 → 1001, originalDataSize 21 → 1002, ownerName 22 → upstream ownerName 20</li>
 *   <li>BucketInfo: compressionType 21 → 1001, raftGroup 22 → 1002</li>
 * </ul>
 * KeyInfo is migrated in keyTable, openKeyTable, fileTable, openFileTable, deletedDirTable, deletedTable
 * (RepeatedKeyInfo) and multipartInfoTable (MultipartKeyInfo parts). Run it once, with the OM stopped, after
 * {@code ozone admin om prepare} and before the first start of the new version, on every OM's DB (and on the
 * OM snapshot checkpoints, if any). The DB is marked as migrated; a second run is refused, because KeyInfo
 * field 20 means the compression type before and the owner after the migration.
 */
@CommandLine.Command(
    name = "sdp-proto-migrate",
    description = "Offline migration of an SDP Ozone 1.4 OM DB to the SDP 1000+ proto numbering of the 2.x line. "
        + "Run once per OM DB with the OM stopped, after 'ozone admin om prepare'.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SdpProtoMigration extends RepairTool {

  static final String MARKER_KEY = "#SDP_PROTO_1000_MIGRATION";
  private static final String META_TABLE = "metaTable";
  private static final int BATCH_SIZE = 10_000;

  private static final Map<String, SdpProtoRewriter> TABLES = new LinkedHashMap<>();

  static {
    TABLES.put("bucketTable", SdpProtoRewriter.BUCKET_INFO);
    TABLES.put("keyTable", SdpProtoRewriter.KEY_INFO);
    TABLES.put("openKeyTable", SdpProtoRewriter.KEY_INFO);
    TABLES.put("fileTable", SdpProtoRewriter.KEY_INFO);
    TABLES.put("openFileTable", SdpProtoRewriter.KEY_INFO);
    TABLES.put("deletedDirectoryTable", SdpProtoRewriter.KEY_INFO);
    TABLES.put("deletedTable", SdpProtoRewriter.REPEATED_KEY_INFO);
    TABLES.put("multipartInfoTable", SdpProtoRewriter.MULTIPART_KEY_INFO);
  }

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "OM DB path (the om.db directory)")
  private String dbPath;

  @Override
  protected Component serviceToBeOffline() {
    return Component.OM;
  }

  @Override
  public void execute() throws Exception {
    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();
    try (ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
        configOptions, dbOptions, dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle metaTable = RocksDBUtils.getColumnFamilyHandle(META_TABLE, cfHandleList);
      if (metaTable == null) {
        throw new IllegalArgumentException(dbPath + " is not an OM DB: no " + META_TABLE);
      }
      byte[] marker = db.get().get(metaTable, MARKER_KEY.getBytes(StandardCharsets.UTF_8));
      if (marker != null) {
        error("%s was already migrated (%s); refusing to run again.", dbPath,
            new String(marker, StandardCharsets.UTF_8));
        return;
      }
      SdpProtoRewriter.Stats stats = migrate(db, cfHandleList, !isDryRun());
      for (Map.Entry<String, Long> e : stats.getCounters().entrySet()) {
        info("%s: %d", e.getKey(), e.getValue());
      }
      for (Map.Entry<String, Long> e : stats.getMovedCompressionValues().entrySet()) {
        info("compression type '%s' moved to field 1001: %d record(s)", e.getKey(), e.getValue());
      }
      if (isDryRun()) {
        info("Dry run: nothing written.");
      } else {
        db.get().put(metaTable, MARKER_KEY.getBytes(StandardCharsets.UTF_8),
            ("migrated at " + java.time.Instant.now()).getBytes(StandardCharsets.UTF_8));
        info("Migration of %s completed.", dbPath);
      }
    } catch (RocksDBException e) {
      error("Failed to migrate the RocksDB at %s", dbPath);
      throw new IOException("Failed to migrate RocksDB.", e);
    } finally {
      IOUtils.closeQuietly(configOptions);
      IOUtils.closeQuietly(dbOptions);
      IOUtils.closeQuietly(cfHandleList);
    }
  }

  static SdpProtoRewriter.Stats migrate(ManagedRocksDB db, List<ColumnFamilyHandle> cfHandleList, boolean write)
      throws RocksDBException {
    SdpProtoRewriter.Stats stats = new SdpProtoRewriter.Stats();
    for (Map.Entry<String, SdpProtoRewriter> table : TABLES.entrySet()) {
      ColumnFamilyHandle cfh = RocksDBUtils.getColumnFamilyHandle(table.getKey(), cfHandleList);
      if (cfh == null) {
        continue;
      }
      migrateTable(db, cfh, table.getValue(), stats, write);
    }
    return stats;
  }

  private static void migrateTable(ManagedRocksDB db, ColumnFamilyHandle cfh, SdpProtoRewriter rewriter,
      SdpProtoRewriter.Stats stats, boolean write) throws RocksDBException {
    try (ManagedRocksIterator it = ManagedRocksIterator.managed(db.get().newIterator(cfh));
         ManagedWriteOptions writeOptions = new ManagedWriteOptions()) {
      ManagedWriteBatch batch = new ManagedWriteBatch();
      int pending = 0;
      try {
        for (it.get().seekToFirst(); it.get().isValid(); it.get().next()) {
          byte[] rewritten = rewriter.rewrite(it.get().value(), stats);
          if (rewritten != null && write) {
            batch.put(cfh, it.get().key(), rewritten);
            if (++pending == BATCH_SIZE) {
              db.get().write(writeOptions, batch);
              batch.close();
              batch = new ManagedWriteBatch();
              pending = 0;
            }
          }
        }
        if (pending > 0) {
          db.get().write(writeOptions, batch);
        }
      } finally {
        batch.close();
      }
    }
  }
}
