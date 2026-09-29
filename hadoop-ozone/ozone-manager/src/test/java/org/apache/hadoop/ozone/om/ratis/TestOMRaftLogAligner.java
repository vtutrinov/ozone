/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMRaftLogInconsistencyException;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OMRaftLogAligner}: the DB-ahead-of-raft-log state
 * (HDDS-15068) must be repaired by deleting the stale segments, a consistent
 * log must be left alone, an inconsistent log that the DB fully covers must
 * be deleted, and one that holds unapplied entries must fail fast.
 */
class TestOMRaftLogAligner {

  private static final long SEGMENT_MAX = 4L * 1024 * 1024;
  private static final int PREALLOCATED = 64 * 1024;
  private static final int BUF_SIZE = 1024 * 1024;
  private static final SizeInBytes MAX_OP_SIZE = SizeInBytes.valueOf("4MB");
  private static final String GROUP = "test-group";

  @TempDir
  private Path tmp;

  private RaftStorage storage;
  private File currentDir;

  @BeforeEach
  void setUp() throws IOException {
    storage = RaftStorage.newBuilder()
        .setDirectory(tmp.resolve("group").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION)
        .setStorageFreeSpaceMin(SizeInBytes.valueOf(0))
        .build();
    storage.initialize();
    currentDir = storage.getStorageDir().getCurrentDir();
  }

  @AfterEach
  void tearDown() throws IOException {
    storage.close();
  }

  @Test
  void consistentLogIsLeftAlone() throws Exception {
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));

    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(230), MAX_OP_SIZE, GROUP));

    assertEquals(Arrays.asList("log_100-199", "log_inprogress_200"), segmentFileNames());
  }

  @Test
  void dbAtExactLogEndIsConsistent() throws Exception {
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));

    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(249), MAX_OP_SIZE, GROUP));
    assertEquals(2, segmentFileNames().size());
  }

  @Test
  void emptyLogOrMissingTransactionInfoIsNoop() throws Exception {
    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(500), MAX_OP_SIZE, GROUP));

    writeClosedSegment(0, 9);
    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, null, MAX_OP_SIZE, GROUP));
    assertEquals(1, segmentFileNames().size());
  }

  @Test
  void dbAheadOfClosedSegmentsDeletesThem() throws Exception {
    writeClosedSegment(100, 199);
    writeClosedSegment(200, 299);

    assertEquals(2, OMRaftLogAligner.alignStaleRaftLog(storage, ti(500), MAX_OP_SIZE, GROUP));

    assertTrue(segmentFileNames().isEmpty(), "current dir must be empty");
  }

  @Test
  void dbAheadOfOpenSegmentDeletesEverything() throws Exception {
    // The production shape: DB replaced by a snapshot at S while the open
    // segment still ends at E < S.
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));

    assertEquals(2, OMRaftLogAligner.alignStaleRaftLog(storage, ti(1000), MAX_OP_SIZE, GROUP));

    assertTrue(segmentFileNames().isEmpty());
  }

  @Test
  void dbAheadOfEmptyOpenSegmentDeletesEverything() throws Exception {
    writeClosedSegment(100, 199);
    writeOpenSegment(200, new ArrayList<>());

    assertEquals(2, OMRaftLogAligner.alignStaleRaftLog(storage, ti(300), MAX_OP_SIZE, GROUP));
    assertTrue(segmentFileNames().isEmpty());
  }

  @Test
  void dbInsideOpenSegmentIsConsistent() throws Exception {
    // DB index past the closed segments but inside the open one: the open
    // segment has to be read to know that.
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));

    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(240), MAX_OP_SIZE, GROUP));
    assertEquals(2, segmentFileNames().size());
  }

  @Test
  void preflightAlignsAndThenPassesGapCheck() throws Exception {
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));

    OMRaftLogAligner.preflight(storage, ti(1000), new RaftProperties(),
        new OzoneConfiguration(), GROUP);

    assertTrue(segmentFileNames().isEmpty());
  }

  @Test
  void preflightAlignsEvenWhenGapCheckDisabled() throws Exception {
    writeClosedSegment(100, 199);
    writeOpenSegment(200, range(200, 249));
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED, false);

    OMRaftLogAligner.preflight(storage, ti(1000), new RaftProperties(), conf, GROUP);

    assertTrue(segmentFileNames().isEmpty(), "alignment must not depend on the switch");
  }

  @Test
  void gapCheckIsSkippedWhenDisabled() throws Exception {
    writeClosedSegment(0, 99);
    writeOpenSegment(150, range(150, 160));
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED, false);

    OMRaftLogAligner.preflight(storage, ti(120), new RaftProperties(), conf, GROUP);

    assertEquals(2, segmentFileNames().size());
  }

  @Test
  void interSegmentGapFailsFast() throws Exception {
    writeClosedSegment(0, 99);
    writeOpenSegment(150, range(150, 160));

    OMRaftLogInconsistencyException e = assertThrows(OMRaftLogInconsistencyException.class,
        () -> OMRaftLogAligner.preflight(storage, ti(120), new RaftProperties(),
            new OzoneConfiguration(), GROUP));
    assertTrue(e.getMessage().contains("inter-segment gap"), e.getMessage());
    assertTrue(e.getMessage().contains("--index 99"), e.getMessage());
  }

  @Test
  void openSegmentFollowedByClosedSegmentFailsFast() throws Exception {
    writeOpenSegment(0, range(0, 9));
    writeClosedSegment(10, 19);

    OMRaftLogInconsistencyException e = assertThrows(OMRaftLogInconsistencyException.class,
        () -> OMRaftLogAligner.failOnInconsistentLog(storage, ti(5), MAX_OP_SIZE, GROUP));
    assertTrue(e.getMessage().contains("corrupted segment layout"), e.getMessage());
  }

  @Test
  void interSegmentGapCoveredByDbIsDeleted() throws Exception {
    writeClosedSegment(0, 99);
    writeOpenSegment(150, range(150, 160));

    // Highest index on disk is 160 and the DB already holds it.
    OMRaftLogAligner.preflight(storage, ti(160), new RaftProperties(),
        new OzoneConfiguration(), GROUP);

    assertTrue(segmentFileNames().isEmpty());
  }

  @Test
  void corruptOpenSegmentCoveredByDbIsDeleted() throws Exception {
    // The production shape after the bug: E followed by S+1 in one file,
    // with the DB far past both.
    List<LogEntryProto> entries = range(100, 105);
    entries.addAll(range(300, 302));
    writeOpenSegment(100, entries);

    assertEquals(1, OMRaftLogAligner.alignStaleRaftLog(storage, ti(500), MAX_OP_SIZE, GROUP));
    assertTrue(segmentFileNames().isEmpty());
  }

  @Test
  void corruptOpenSegmentWithUnappliedEntriesFailsFast() throws Exception {
    List<LogEntryProto> entries = range(100, 105);
    entries.addAll(range(300, 302));
    writeOpenSegment(100, entries);

    // DB at 200: entries 300..302 are on disk but not applied.
    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(200), MAX_OP_SIZE, GROUP));
    OMRaftLogInconsistencyException e = assertThrows(OMRaftLogInconsistencyException.class,
        () -> OMRaftLogAligner.preflight(storage, ti(200), new RaftProperties(),
            new OzoneConfiguration(), GROUP));
    assertTrue(e.getMessage().contains("is corrupt"), e.getMessage());
    assertTrue(e.getMessage().contains("entries 201..302"), e.getMessage());
    assertEquals(1, segmentFileNames().size(), "nothing may be deleted when entries are unapplied");
  }

  @Test
  void unreadableOpenSegmentFailsFastWhateverTheDbIndex() throws Exception {
    writeOpenSegment(100, range(100, 199));
    File segment = new File(currentDir, "log_inprogress_100");
    try (RandomAccessFile raf = new RandomAccessFile(segment, "rw")) {
      // Damage the payload of an entry in the middle; later entries stay valid.
      raf.seek(400);
      raf.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    }

    OMRaftLogInconsistencyException e = assertThrows(OMRaftLogInconsistencyException.class,
        () -> OMRaftLogAligner.preflight(storage, ti(100_000), new RaftProperties(),
            new OzoneConfiguration(), GROUP));
    assertTrue(e.getMessage().contains("is corrupt"), e.getMessage());
    assertTrue(e.getMessage().contains("cannot be read to the end"), e.getMessage());
    assertEquals(1, segmentFileNames().size());
  }

  @Test
  void corruptOpenSegmentIsNotReadWhenClosedSegmentsCoverDb() throws Exception {
    writeClosedSegment(0, 99);
    List<LogEntryProto> entries = range(100, 105);
    entries.addAll(range(300, 302));
    writeOpenSegment(100, entries);

    assertEquals(0, OMRaftLogAligner.alignStaleRaftLog(storage, ti(50), MAX_OP_SIZE, GROUP));
    assertEquals(2, segmentFileNames().size());
  }

  // --- helpers ---

  private static TermIndex ti(long index) {
    return TermIndex.valueOf(1, index);
  }

  private List<String> segmentFileNames() {
    return sortedNames(currentDir).stream()
        .filter(n -> n.startsWith("log_"))
        .collect(Collectors.toList());
  }

  private static List<String> sortedNames(File dir) {
    File[] files = dir.listFiles();
    assertNotNull(files);
    return Arrays.stream(files).map(File::getName).sorted().collect(Collectors.toList());
  }

  private static List<LogEntryProto> range(int startIdx, int endIdx) {
    List<LogEntryProto> list = new ArrayList<>();
    for (int i = startIdx; i <= endIdx; i++) {
      list.add(LogEntryProto.newBuilder()
          .setTerm(1)
          .setIndex(i)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
              .setCallId(i)
              .setClientId(ByteString.copyFromUtf8("test"))
              .setLogData(ByteString.copyFromUtf8("e" + i))
              .build())
          .build());
    }
    return list;
  }

  private void writeClosedSegment(long startIdx, long endIdx) throws IOException {
    writeSegment(new File(currentDir, "log_" + startIdx + "-" + endIdx),
        range((int) startIdx, (int) endIdx));
  }

  private void writeOpenSegment(long startIdx, List<LogEntryProto> entries) throws IOException {
    writeSegment(new File(currentDir, "log_inprogress_" + startIdx), entries);
  }

  private static void writeSegment(File file, List<LogEntryProto> entries) throws IOException {
    ByteBuffer buf = ByteBuffer.allocateDirect(BUF_SIZE);
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(
        file, false, SEGMENT_MAX, PREALLOCATED, buf)) {
      for (LogEntryProto e : entries) {
        out.write(e);
      }
      out.flush();
    }
  }
}
