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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMRaftLogInconsistencyException;
import org.apache.hadoop.ozone.repair.om.raftlog.RaftLogRepair;
import org.apache.hadoop.ozone.repair.om.raftlog.RaftLogTruncate;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression test for the raft-log gap recovery flow (HDDS-15068 family).
 *
 * Reproduces the operational scenarios behind the
 * {@code bugfix/raft-log-entry-gap} branch:
 * <ul>
 *   <li>A follower OM that misses a stretch of log entries should still come
 *       back cleanly via install-snapshot; our install-snapshot race guard in
 *       {@link OzoneManager#installCheckpoint} should not break that.</li>
 *   <li>A follower OM whose raft log is left with a gap (the actual prod
 *       failure mode) should fail startup fast with
 *       {@link OMRaftLogInconsistencyException} that points at the
 *       {@code ozone repair om raft-log} tool, and the tool should bring it
 *       back to a startable state.</li>
 * </ul>
 */
public class TestOzoneManagerRaftLogGapRecovery extends TestOzoneManagerHA {

  @BeforeEach
  void waitLeader() throws Exception {
    waitForLeaderToBeReady();
  }

  @AfterEach
  void resetCluster() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    if (cluster != null) {
      cluster.restartOzoneManager();
    }
  }

  @Test
  void stoppedFollowerRecoversViaInstallSnapshot() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader);
    OzoneManager follower = pickFollower(cluster, leader);

    cluster.stopOzoneManager(follower.getOMNodeId());

    // Drive enough write traffic to cross SNAPSHOT_THRESHOLD + LOG_PURGE_GAP
    // (50 + 50 in the base class) so the offline OM cannot catch up via the
    // raft log on restart and must use install-snapshot.
    createKeys(150);

    cluster.startInactiveOM(follower.getOMNodeId());

    // The new install-snapshot race guard must not break the happy path:
    // after restart the follower's applied index should catch up to within a
    // small margin of the leader's.
    long leaderIdx = leader.getOmRatisServer().getLastAppliedTermIndex()
        .getIndex();
    OzoneManager restarted = cluster.getOzoneManager(follower.getOMNodeId());
    org.apache.ozone.test.GenericTestUtils.waitFor(() -> {
      try {
        return restarted.getOmRatisServer().getLastAppliedTermIndex()
            .getIndex() >= leaderIdx;
      } catch (Exception e) {
        return false;
      }
    }, 500, 60_000);
  }

  @Test
  void corruptedRaftLogFailsFastAndRecoversViaRepairTool() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager leader = cluster.getOMLeader();
    OzoneManager follower = pickFollower(cluster, leader);

    cluster.stopOzoneManager(follower.getOMNodeId());

    // Inject an inter-segment gap into the offline follower's raft log: drop
    // a hand-crafted segment file whose startIndex is far past the current
    // last index. The pre-flight scan in OzoneManagerStateMachine should
    // refuse to start with OMRaftLogInconsistencyException.
    File raftLogRoot = new File(follower.getOmRatisServer().getRatisStorageDir());
    File currentDir = findFirstCurrentDir(raftLogRoot);
    assertNotNull(currentDir,
        "could not find any 'current' raft-log dir under " + raftLogRoot);
    long gapStart = 9_000_000L;
    long gapEnd = gapStart + 4L;
    writeStandaloneClosedSegment(currentDir, gapStart, gapEnd);

    try {
      cluster.startInactiveOM(follower.getOMNodeId());
      fail("expected OMRaftLogInconsistencyException on startup, got none");
    } catch (Exception e) {
      assertTrue(causeChainContains(e, OMRaftLogInconsistencyException.class),
          "expected OMRaftLogInconsistencyException in cause chain, got: " + e);
    }

    // Repair: truncate the bogus segment by running the new tool
    // programmatically. After repair the OM must start cleanly.
    runRaftLogTruncate(raftLogRoot, gapStart - 1L);

    cluster.startInactiveOM(follower.getOMNodeId());
    OzoneManager restarted = cluster.getOzoneManager(follower.getOMNodeId());
    org.apache.ozone.test.GenericTestUtils.waitFor(restarted::isRunning,
        500, 60_000);
  }

  // --- helpers ---

  private OzoneManager pickFollower(MiniOzoneHAClusterImpl cluster,
      OzoneManager leader) {
    List<OzoneManager> all = cluster.getOzoneManagersList();
    for (OzoneManager om : all) {
      if (!om.getOMNodeId().equals(leader.getOMNodeId())) {
        return om;
      }
    }
    throw new IllegalStateException("no follower OM found");
  }

  private void createKeys(int numKeys) throws Exception {
    String volumeName = "vol" + UUID.randomUUID().toString().substring(0, 8);
    String bucketName = "bkt" + UUID.randomUUID().toString().substring(0, 8);
    VolumeArgs vargs = VolumeArgs.newBuilder().setOwner("test").setAdmin("test")
        .build();
    getObjectStore().createVolume(volumeName, vargs);
    OzoneVolume volume = getObjectStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    byte[] value = "x".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    for (int i = 0; i < numKeys; i++) {
      String keyName = "k" + i + "-" + UUID.randomUUID();
      try (OzoneOutputStream out = bucket.createKey(keyName, value.length,
          ReplicationType.RATIS, ReplicationFactor.ONE, new HashMap<>())) {
        out.write(value);
      }
    }
  }

  private File findFirstCurrentDir(File root) {
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

  private void writeStandaloneClosedSegment(File dir, long startIdx, long endIdx)
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
  }

  private void runRaftLogTruncate(File raftLogRoot, long lastGoodIndex)
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
