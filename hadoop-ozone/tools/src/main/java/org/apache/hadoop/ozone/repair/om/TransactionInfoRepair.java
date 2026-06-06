/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.repair.om;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.RDBRepair;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * {@code ozone repair ldb --db <om.db> transaction-info}: show or update the
 * OM RocksDB {@code TRANSACTION_INFO_KEY} row in {@code transactionInfoTable}.
 * Use after {@code ozone repair om raft-log truncate} to bring the persisted
 * applied index back into the raft log's index range. The OM must NOT be
 * running against this RocksDB.
 *
 * <ul>
 *   <li>Default: show the current term/index.</li>
 *   <li>With both {@code --set-term} and {@code --set-index}: propose
 *       (or commit, with {@code --dry-run=false}) the new value.</li>
 * </ul>
 */
@CommandLine.Command(name = "transaction-info",
    description = "Show or rewrite the OM RocksDB TRANSACTION_INFO_KEY row "
        + "(used to recover from raft-log/transaction-info index mismatches).")
@MetaInfServices(SubcommandWithParent.class)
public class TransactionInfoRepair implements Callable<Void>, SubcommandWithParent {

  private static final String TXN_INFO_CF = "transactionInfoTable";

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Option(names = {"--set-term"},
      description = "New raft term to write. Must be paired with --set-index.")
  private Long setTerm;

  @CommandLine.Option(names = {"--set-index"},
      description = "New transaction index to write. Must be paired with --set-term.")
  private Long setIndex;

  @CommandLine.Option(names = {"--dry-run"},
      description = "When setting, show the planned mutation without applying it.",
      defaultValue = "true")
  private boolean dryRun;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescs =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    try (ManagedRocksDB db = ManagedRocksDB.open(parent.getDbPath(), cfDescs,
        cfHandles)) {
      ColumnFamilyHandle cf = findCf(cfHandles);
      if (cf == null) {
        System.err.println(TXN_INFO_CF + " is not a column family in the DB at "
            + parent.getDbPath());
        return null;
      }
      TransactionInfo current = read(db, cf);
      System.out.println("Current TransactionInfo: " + (current == null
          ? "<absent>" : current.getTermIndex()));

      if (setTerm == null && setIndex == null) {
        return null;
      }
      if (setTerm == null || setIndex == null) {
        System.err.println("--set-term and --set-index must be provided together.");
        return null;
      }
      TransactionInfo proposed =
          TransactionInfo.valueOf(setTerm, setIndex);
      if (dryRun) {
        System.out.println("Would set TransactionInfo to: " + proposed.getTermIndex()
            + " (dry-run; rerun with --dry-run=false to apply)");
        return null;
      }
      System.out.println("Make sure the OM is not running against this RocksDB.");
      byte[] keyBytes = StringCodec.get().toPersistedFormat(TRANSACTION_INFO_KEY);
      byte[] valueBytes = proposed.convertToByteArray();
      db.get().put(cf, keyBytes, valueBytes);
      System.out.println("TransactionInfo updated to: " + proposed.getTermIndex());
    } catch (RocksDBException e) {
      System.err.println("Failed RocksDB operation on " + parent.getDbPath()
          + ": " + e.getMessage());
      System.err.println(
          "Make sure that no Ozone entity is running against this dbPath.");
    } finally {
      IOUtils.closeQuietly(cfHandles);
    }
    return null;
  }

  private ColumnFamilyHandle findCf(List<ColumnFamilyHandle> handles)
      throws RocksDBException {
    byte[] nameBytes = TXN_INFO_CF.getBytes(StandardCharsets.UTF_8);
    for (ColumnFamilyHandle cf : handles) {
      if (Arrays.equals(cf.getName(), nameBytes)) {
        return cf;
      }
    }
    return null;
  }

  private TransactionInfo read(ManagedRocksDB db, ColumnFamilyHandle cf)
      throws RocksDBException {
    byte[] keyBytes;
    try {
      keyBytes = StringCodec.get().toPersistedFormat(TRANSACTION_INFO_KEY);
    } catch (java.io.IOException e) {
      throw new RocksDBException(e.getMessage());
    }
    byte[] valueBytes = db.get().get(cf, keyBytes);
    if (valueBytes == null) {
      return null;
    }
    return TransactionInfo.getFromByteArray(valueBytes);
  }

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }
}
