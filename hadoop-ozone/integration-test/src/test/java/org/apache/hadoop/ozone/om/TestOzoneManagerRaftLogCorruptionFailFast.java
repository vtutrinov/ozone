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
package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.om.exceptions.OMRaftLogInconsistencyException;
import org.apache.hadoop.ozone.repair.om.raftlog.RaftLogRepair;
import org.apache.hadoop.ozone.repair.om.raftlog.RaftLogTruncate;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.om.TestOzoneManagerRaftLogGapRecovery.pickFollower;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A follower OM whose raft log is inconsistent and holds entries above its DB
 * index (so discarding the log would lose unapplied entries) must fail startup
 * fast with {@link OMRaftLogInconsistencyException} that
 * points at the {@code ozone repair om raft-log} tool, and the tool must drop
 * the segments past the last good index.
 *
 * Kept in its own class: an OM whose start failed cannot be restarted again
 * inside the same mini cluster, so this must be the last thing that happens
 * to the cluster.
 */
public class TestOzoneManagerRaftLogCorruptionFailFast extends TestOzoneManagerHA {

  @BeforeEach
  void waitLeader() throws Exception {
    waitForLeaderToBeReady();
  }

  @Test
  void corruptedRaftLogFailsFastAndRepairToolRestoresLayout() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader);
    OzoneManager follower = pickFollower(cluster, leader);
    File raftLogRoot = new File(follower.getOmRatisServer().getRatisStorageDir());
    // Last index really present in the follower's raft log: the value an
    // operator gets from 'ozone repair om raft-log inspect'. Right after the
    // leader became ready the follower's log may still be empty; wait for it.
    RaftLog followerLog = follower.getOmRatisServer()
        .getServerDivision(follower.getOmRatisServer().getCurrentRaftGroupId()).getRaftLog();
    GenericTestUtils.waitFor(() -> followerLog.getLastEntryTermIndex() != null, 200, 30_000);
    long lastGoodIndex = followerLog.getLastEntryTermIndex().getIndex();

    cluster.shutdownOzoneManager(follower);

    // Inject an inter-segment gap into the offline follower's raft log: drop
    // a hand-crafted segment file whose startIndex is far past the current
    // last index and therefore far past the DB index. Those entries are not
    // applied, so the pre-flight in OMRaftLogAligner must not repair the log
    // on its own and must refuse to start.
    File currentDir = findFirstCurrentDir(raftLogRoot);
    assertNotNull(currentDir,
        "could not find any 'current' raft-log dir under " + raftLogRoot);
    long gapStart = 9_000_000L;
    long gapEnd = gapStart + 4L;
    File bogus = writeStandaloneClosedSegment(currentDir, gapStart, gapEnd);

    try {
      cluster.restartOzoneManager(follower, false);
      fail("expected OMRaftLogInconsistencyException on startup, got none");
    } catch (Exception e) {
      assertTrue(causeChainContains(e, OMRaftLogInconsistencyException.class),
          "expected OMRaftLogInconsistencyException in cause chain, got: " + e);
    }
    assertTrue(bogus.exists(), "fail-fast must not touch the segment files");

    // Repair: truncate the bogus segment by running the tool programmatically.
    // The failed OM instance still holds the Ratis storage lock, so verify the
    // repaired layout on the file level: nothing may start past the target.
    runRaftLogTruncate(raftLogRoot, lastGoodIndex);
    assertFalse(bogus.exists(), "truncate must drop the segment past the target index");
    for (LogSegmentPath segment : listSegments(currentDir)) {
      assertTrue(segment.getStartEnd().getStartIndex() <= lastGoodIndex,
          "segment past the last good index survived: " + segment.getPath());
      long end = segment.getStartEnd().isOpen() ? lastGoodIndex : segment.getStartEnd().getEndIndex();
      assertTrue(end <= lastGoodIndex, "segment ends past the last good index: " + segment.getPath());
    }
  }

  // --- helpers ---

  private static List<LogSegmentPath> listSegments(File currentDir) {
    List<LogSegmentPath> segments = new ArrayList<>();
    File[] files = currentDir.listFiles();
    assertNotNull(files);
    for (File f : files) {
      LogSegmentPath segment = LogSegmentPath.matchLogSegment(f.toPath());
      if (segment != null) {
        segments.add(segment);
      }
    }
    return segments;
  }

  static File findFirstCurrentDir(File root) {
    if (!root.isDirectory()) {
      return null;
    }
    for (File child : root.listFiles()) {
      if (!child.isDirectory()) {
        continue;
      }
      File current = new File(child, "current");
      if (current.isDirectory()) {
        return current;
      }
      File deeper = findFirstCurrentDir(child);
      if (deeper != null) {
        return deeper;
      }
    }
    return null;
  }

  static File writeStandaloneClosedSegment(File dir, long startIdx, long endIdx)
      throws IOException {
    File f = new File(dir, String.format("log_%d-%d", startIdx, endIdx));
    ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(
        f, false, 4 * 1024 * 1024, 4 * 1024 * 1024, buf)) {
      for (long idx = startIdx; idx <= endIdx; idx++) {
        out.write(LogEntryProto.newBuilder()
            .setTerm(1)
            .setIndex(idx)
            .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
                .setCallId(idx)
                .setClientId(ByteString.copyFromUtf8("inject"))
                .setLogData(ByteString.copyFromUtf8("e" + idx))
                .build())
            .build());
      }
      out.flush();
    }
    return f;
  }

  private static void runRaftLogTruncate(File raftLogRoot, long lastGoodIndex)
      throws Exception {
    RaftLogRepair parent = new RaftLogRepair();
    new CommandLine(parent);
    java.lang.reflect.Field f = RaftLogRepair.class.getDeclaredField("raftLogDir");
    f.setAccessible(true);
    f.set(parent, raftLogRoot.getAbsolutePath());

    RaftLogTruncate truncate = new RaftLogTruncate();
    java.lang.reflect.Field p = RaftLogTruncate.class.getDeclaredField("parent");
    p.setAccessible(true);
    p.set(truncate, parent);
    java.lang.reflect.Field idx = RaftLogTruncate.class.getDeclaredField("targetIndex");
    idx.setAccessible(true);
    idx.set(truncate, lastGoodIndex);
    java.lang.reflect.Field dry = RaftLogTruncate.class.getDeclaredField("dryRun");
    dry.setAccessible(true);
    dry.set(truncate, false);

    truncate.call();
  }

  private static boolean causeChainContains(Throwable top, Class<?> target) {
    for (Throwable t = top; t != null; t = t.getCause()) {
      if (target.isInstance(t)) {
        return true;
      }
    }
    return false;
  }
}
