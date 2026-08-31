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

package org.apache.hadoop.ozone.repair.om.raftlog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the three failure modes the prod crash needs:
 *   (a) clean log -> no gaps reported,
 *   (b) inter-segment gap -> reported,
 *   (c) intra-segment gap -> reported (matches HDDS-15068 crash mode),
 * plus a truncate round-trip that deletes a too-far segment and rewrites
 * the segment containing the target index.
 */
class TestRaftLogInspectTruncate {

  private static final int SEGMENT_MAX = 4 * 1024 * 1024;
  private static final int PREALLOCATED = 4 * 1024 * 1024;
  private static final int BUF_SIZE = 8 * 1024 * 1024;

  @Test
  void inspectsCleanLogWithoutGaps(@TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-A/current");
    Files.createDirectories(groupDir);
    writeClosedSegment(groupDir, 0, range(0, 100));
    writeClosedSegment(groupDir, 101, range(101, 200));

    String stdout = runInspect(tmp);
    assertTrue(stdout.contains("No gaps detected."), stdout);
    assertFalse(stdout.contains("inter-segment gap"), stdout);
    assertFalse(stdout.contains("intra-segment gap"), stdout);
  }

  @Test
  void inspectDetectsInterSegmentGap(@TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-B/current");
    Files.createDirectories(groupDir);
    writeClosedSegment(groupDir, 0, range(0, 100));
    // Gap: next segment starts at 150 instead of 101.
    writeClosedSegment(groupDir, 150, range(150, 200));

    String stdout = runInspect(tmp);
    assertTrue(stdout.contains("inter-segment gap"), stdout);
    assertTrue(stdout.contains("Detected"), stdout);
  }

  @Test
  void inspectDetectsIntraSegmentGap(@TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-C/current");
    Files.createDirectories(groupDir);
    // Mirrors the HDDS-15068 prod crash: entries within one file jump
    // from index 50 straight to index 100.
    List<LogEntryProto> entries = new ArrayList<>();
    entries.addAll(range(0, 50));
    entries.addAll(rangeFromTerm(100, 150, 1));
    writeClosedSegment(groupDir, 0, 149, entries);

    String stdout = runInspect(tmp);
    assertTrue(stdout.contains("intra-segment gap"), stdout);
  }

  @Test
  void truncateRewritesContainingSegmentAndDeletesLater(
      @TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-D/current");
    Files.createDirectories(groupDir);
    writeClosedSegment(groupDir, 0, range(0, 99));
    writeClosedSegment(groupDir, 100, range(100, 199));
    writeClosedSegment(groupDir, 200, range(200, 299));

    // Truncate keeping entries [0, 150]: segment 100-199 is rewritten as
    // 100-150; segment 200-299 is deleted.
    runTruncate(tmp, 150L, false);

    assertTrue(new File(groupDir.toFile(), "log_0-99").exists());
    assertTrue(new File(groupDir.toFile(), "log_100-150").exists());
    assertFalse(new File(groupDir.toFile(), "log_100-199").exists());
    assertFalse(new File(groupDir.toFile(), "log_200-299").exists());

    // Re-inspect after truncate: should be clean.
    String inspectStdout = runInspect(tmp);
    assertTrue(inspectStdout.contains("No gaps detected."), inspectStdout);
  }

  @Test
  void truncateRefusesWhenTargetIndexNotInAnySegment(
      @TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-E/current");
    Files.createDirectories(groupDir);
    writeClosedSegment(groupDir, 0, range(0, 50));
    writeClosedSegment(groupDir, 100, range(100, 150));

    String stderr = captureStderr(() -> runTruncate(tmp, 75L, false));
    assertTrue(stderr.contains("refusing"), stderr);
    assertTrue(new File(groupDir.toFile(), "log_0-50").exists());
    assertTrue(new File(groupDir.toFile(), "log_100-150").exists());
  }

  @Test
  void truncateDryRunDoesNotMutate(@TempDir Path tmp) throws Exception {
    Path groupDir = tmp.resolve("group-F/current");
    Files.createDirectories(groupDir);
    writeClosedSegment(groupDir, 0, range(0, 99));
    writeClosedSegment(groupDir, 100, range(100, 199));

    String stdout = runTruncate(tmp, 150L, true);
    assertTrue(stdout.contains("rewrite"), stdout);
    assertTrue(new File(groupDir.toFile(), "log_100-199").exists());
    assertFalse(new File(groupDir.toFile(), "log_100-150").exists());
  }

  // --- helpers ---

  private static List<LogEntryProto> range(int startIdx, int endIdx) {
    return rangeFromTerm(startIdx, endIdx, 1);
  }

  private static List<LogEntryProto> rangeFromTerm(int startIdx, int endIdx,
      int term) {
    List<LogEntryProto> list = new ArrayList<>();
    for (int i = startIdx; i <= endIdx; i++) {
      list.add(makeEntry(term, i));
    }
    return list;
  }

  private static LogEntryProto makeEntry(int term, int index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
            .setCallId(index)
            .setClientId(ByteString.copyFromUtf8("test"))
            .setLogData(ByteString.copyFromUtf8("e" + index))
            .build())
        .build();
  }

  private static void writeClosedSegment(Path dir, long startIdx,
      List<LogEntryProto> entries) throws Exception {
    long endIdx = entries.get(entries.size() - 1).getIndex();
    writeClosedSegment(dir, startIdx, endIdx, entries);
  }

  private static void writeClosedSegment(Path dir, long startIdx, long endIdx,
      List<LogEntryProto> entries) throws Exception {
    File f = dir.resolve(String.format("log_%d-%d", startIdx, endIdx)).toFile();
    ByteBuffer buf = ByteBuffer.allocateDirect(BUF_SIZE);
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(
        f, false, SEGMENT_MAX, PREALLOCATED, buf)) {
      for (LogEntryProto e : entries) {
        out.write(e);
      }
      out.flush();
    }
  }

  private static String runInspect(Path raftLogDir) throws Exception {
    RaftLogRepair parent = new RaftLogRepair();
    new CommandLine(parent);
    setRaftLogDir(parent, raftLogDir.toString());
    RaftLogInspect inspect = new RaftLogInspect();
    setField(inspect, "parent", parent);
    return captureStdout(inspect);
  }

  private static String runTruncate(Path raftLogDir, long index, boolean dryRun)
      throws Exception {
    RaftLogRepair parent = new RaftLogRepair();
    new CommandLine(parent);
    setRaftLogDir(parent, raftLogDir.toString());
    RaftLogTruncate truncate = new RaftLogTruncate();
    setField(truncate, "parent", parent);
    setField(truncate, "targetIndex", index);
    setField(truncate, "dryRun", dryRun);
    return captureStdout(truncate);
  }

  private static String captureStdout(java.util.concurrent.Callable<?> cmd)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream saved = System.out;
    System.setOut(new PrintStream(baos, true, StandardCharsets.UTF_8.name()));
    try {
      cmd.call();
    } finally {
      System.setOut(saved);
    }
    return baos.toString(StandardCharsets.UTF_8.name());
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static String captureStderr(ThrowingRunnable r) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream saved = System.err;
    System.setErr(new PrintStream(baos, true, StandardCharsets.UTF_8.name()));
    try {
      r.run();
    } finally {
      System.setErr(saved);
    }
    return baos.toString(StandardCharsets.UTF_8.name());
  }

  private static void setRaftLogDir(RaftLogRepair parent, String dir)
      throws Exception {
    setField(parent, "raftLogDir", dir);
    assertEquals(dir, parent.getRaftLogDir());
  }

  private static void setField(Object target, String name, Object value)
      throws Exception {
    java.lang.reflect.Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
