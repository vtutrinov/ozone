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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.ratis.OMRaftLogAligner;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the raft-log gap family (HDDS-15068 / HDDS-15103).
 * <ul>
 *   <li>A follower OM that misses a stretch of log entries must still come
 *       back cleanly via install-snapshot; the startup pre-flight in
 *       {@link OMRaftLogAligner} must not break that happy path.</li>
 *   <li>A follower OM whose DB is ahead of its raft log (the state the OM is
 *       left in when it dies between installing a leader checkpoint and
 *       purging its raft log) must align the raft log at startup instead of
 *       letting Ratis append into the stale open segment, which is what
 *       corrupts the segment file and kills every following start.</li>
 * </ul>
 * The fail-fast + repair-tool path for an already corrupt log lives in
 * {@link TestOzoneManagerRaftLogCorruptionFailFast}, because an OM whose
 * start failed cannot be restarted again inside the same mini cluster.
 */
public class TestOzoneManagerRaftLogGapRecovery extends TestOzoneManagerHA {

  @BeforeEach
  void waitLeader() throws Exception {
    waitForLeaderToBeReady();
  }

  @Test
  void stoppedFollowerRecoversViaInstallSnapshot() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader);
    OzoneManager follower = pickFollower(cluster, leader);

    cluster.shutdownOzoneManager(follower);

    // Drive enough write traffic to cross SNAPSHOT_THRESHOLD + LOG_PURGE_GAP
    // (50 + 50 in the base class) so the offline OM cannot catch up via the
    // raft log on restart and must use install-snapshot.
    createKeys(150);

    cluster.restartOzoneManager(follower, true);

    waitForCatchUp(follower, leader);
  }

  @Test
  void dbAheadOfRaftLogIsAlignedOnStartup() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader);
    OzoneManager follower = pickFollower(cluster, leader);
    File followerDb = follower.getMetadataManager().getStore().getDbLocation();
    File raftLogRoot = new File(follower.getOmRatisServer().getRatisStorageDir());

    cluster.shutdownOzoneManager(follower);

    // Move the leader well past the stopped follower's raft log.
    createKeys(150);

    // Reproduce the HDDS-15068 state: replace the follower's DB with a leader
    // checkpoint (what installCheckpoint does) but leave its raft log alone,
    // so TransactionInfo index S is far past the raft log end index E.
    DBCheckpoint checkpoint = leader.getMetadataManager().getStore().getCheckpoint(true);
    long checkpointIndex;
    try {
      TransactionInfo checkpointTrxnInfo = OzoneManagerRatisUtils.getTrxnInfoFromCheckpoint(
          getConf(), checkpoint.getCheckpointLocation());
      checkpointIndex = checkpointTrxnInfo.getTransactionIndex();
      FileUtils.deleteDirectory(followerDb);
      FileUtils.copyDirectory(checkpoint.getCheckpointLocation().toFile(), followerDb);
    } finally {
      checkpoint.cleanupCheckpoint();
    }

    List<File> staleSegments = listSegmentFiles(raftLogRoot);
    assertTrue(staleSegments.size() > 0, "follower must have raft log segments before restart");

    GenericTestUtils.LogCapturer aligner = GenericTestUtils.LogCapturer.captureLogs(OMRaftLogAligner.LOG);
    cluster.restartOzoneManager(follower, true);

    assertTrue(aligner.getOutput().contains("OM DB index " + checkpointIndex + " is past the raft log end index"),
        aligner.getOutput());
    for (File segment : staleSegments) {
      assertTrue(!segment.exists(), "stale segment must be deleted: " + segment);
    }

    // The follower must catch up through ordinary appends / install-snapshot.
    createKeys(20);
    waitForCatchUp(follower, leader);

    // Without the aligner the leader's first append after the restart is
    // written into the stale open segment right after its last entry and the
    // *next* start dies with "gap between entry ...". Restart once more to pin
    // that regression.
    cluster.shutdownOzoneManager(follower);
    cluster.restartOzoneManager(follower, true);
    waitForCatchUp(follower, leader);
  }

  // --- helpers ---

  private static void waitForCatchUp(OzoneManager follower, OzoneManager leader)
      throws Exception {
    long leaderIdx = leader.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    GenericTestUtils.waitFor(() -> {
      try {
        return follower.getOmRatisServer().getLastAppliedTermIndex().getIndex() >= leaderIdx;
      } catch (Exception e) {
        return false;
      }
    }, 500, 120_000);
  }

  static OzoneManager pickFollower(MiniOzoneHAClusterImpl cluster,
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

  private static List<File> listSegmentFiles(File root) {
    List<File> segments = new ArrayList<>();
    File[] children = root.listFiles();
    if (children == null) {
      return segments;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        segments.addAll(listSegmentFiles(child));
      } else if (child.getName().startsWith("log_")) {
        segments.add(child);
      }
    }
    return segments;
  }
}
