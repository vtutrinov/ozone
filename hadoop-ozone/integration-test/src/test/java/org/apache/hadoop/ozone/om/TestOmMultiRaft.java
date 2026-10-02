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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check OzoneManager multi-raft feature.
 * 
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class TestOmMultiRaft {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmMultiRaft.class);
  private static final String VOLUME_NAME = "testvol";
  private static final String BUCKET_NAME = "testbucket";

  private static MiniOzoneHAClusterImpl cluster = null;
  private static ObjectStore objectStore;
  private static OzoneConfiguration conf;
  private static String omServiceId;
  private static final int NUM_OF_OMS = 3;
  private static final int LOG_PURGE_GAP = 50;
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final Duration RETRY_CACHE_DURATION = Duration.ofSeconds(30);
  private static final int MULTI_RAFT_BUCKET_GROUPS = 4;
  private static OzoneClient client;

  @BeforeAll
  public static void init()
      throws InterruptedException, TimeoutException, IOException {
    conf = new OzoneConfiguration();
    omServiceId = "om-service-test1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE);

    // Enable MultiRaft with 4 bucket raft groups
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, MULTI_RAFT_BUCKET_GROUPS);

    // Enable OM gRPC server — required for OM-to-OM communication used by
    // OmRaftGroupManager (lock acquire/release/assign via Raft).
    // The integration-test ozone-site.xml disables it by default.
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, true);

    OzoneManagerRatisServerConfig omHAConfig =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omHAConfig.setRetryCacheTimeout(RETRY_CACHE_DURATION);
    conf.setFromObject(omHAConfig);

    conf.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "10s");
    conf.set(OZONE_KEY_DELETING_LIMIT_PER_TASK, "2");

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Verifies that {@code BucketRaftGroupAssign} is executed exactly once for
   * the same bucket when multiple concurrent clients send write requests.
   *
   * Uses {@code OmRaftGroupManager#getBucketRaftGroupAssignmentCount()} —
   * a counter incremented every time a {@code BucketRaftGroupAssign} Raft log
   * entry is applied on an OM — to assert that the assignment happened exactly
   * once across all OMs, not just that the final map state is consistent.
   */
  @Test
  void testBucketRaftGroupAssignExecutedOnceForConcurrentWrites()
      throws Exception {
    // 1 main group + MULTI_RAFT_BUCKET_GROUPS bucket groups
    int expectedRaftGroupsCount = 1 + MULTI_RAFT_BUCKET_GROUPS;

    // Wait for all bucket raft groups to be initialized on every OM
    waitFor(
        () -> cluster.getOzoneManager(0).getOmRaftGroups().size()
                == expectedRaftGroupsCount
            && cluster.getOzoneManager(1).getOmRaftGroups().size()
                == expectedRaftGroupsCount
            && cluster.getOzoneManager(2).getOmRaftGroups().size()
                == expectedRaftGroupsCount,
        1000,
        120_000
    );

    // Create volume and bucket (metadata ops go through main raft group;
    // bucket-to-raft-group assignment is deferred until first key write)
    objectStore.createVolume(VOLUME_NAME);
    objectStore.getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);

    OzoneManager omLeader = cluster.getOMLeader();
    String bucketKey = omLeader.getMetadataManager()
        .getBucketKey(VOLUME_NAME, BUCKET_NAME);

    // Record baseline assignment counts on every OM before concurrent writes
    int[] baselineCounts = new int[NUM_OF_OMS];
    for (int i = 0; i < NUM_OF_OMS; i++) {
      baselineCounts[i] = cluster.getOzoneManager(i)
          .getOmRaftGroupManager().getBucketRaftGroupAssignmentCount();
    }

    // Launch N concurrent clients writing keys to the SAME bucket.
    // readyLatch/startLatch form a barrier so all threads fire simultaneously,
    // maximizing the chance of a race in getRaftGroupToHandleBucketWriteRequest.
    int numThreads = 10;
    CountDownLatch readyLatch = new CountDownLatch(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    List<Exception> errors = Collections.synchronizedList(new ArrayList<>());

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      int idx = i;
      futures.add(executor.submit(() -> {
        OzoneClient threadClient = null;
        try {
          threadClient = OzoneClientFactory.getRpcClient(omServiceId, conf);
          // Signal ready and wait for all threads to line up
          readyLatch.countDown();
          startLatch.await();

          String keyName = "concurrent-key-" + idx;
          try (OzoneOutputStream stream = threadClient.getObjectStore()
              .getVolume(VOLUME_NAME)
              .getBucket(BUCKET_NAME)
              .createKey(keyName, keyName.length(),
                  ReplicationConfig.getDefault(conf),
                  Collections.emptyMap())) {
            stream.write(keyName.getBytes(UTF_8));
          }
          successCount.incrementAndGet();
        } catch (Exception e) {
          errors.add(e);
          LOG.error("Thread {} failed", idx, e);
        } finally {
          IOUtils.closeQuietly(threadClient);
        }
      }));
    }

    // Wait for every thread to be ready, then release them at once
    readyLatch.await(60, TimeUnit.SECONDS);
    startLatch.countDown();

    // Wait for all writes to complete
    for (Future<?> future : futures) {
      future.get(120, TimeUnit.SECONDS);
    }
    executor.shutdown();

    // --- Assertions ---

    // All concurrent writes must succeed (no deadlocks or timeouts)
    assertEquals(numThreads, successCount.get(),
        "All concurrent writes should succeed. Errors: " + errors);

    // BucketRaftGroupAssign must have been applied exactly once on every OM.
    // A delta > 1 would mean multiple assign requests leaked through Raft,
    // which could route writes to different raft groups temporarily.
    for (int i = 0; i < NUM_OF_OMS; i++) {
      OzoneManager om = cluster.getOzoneManager(i);
      int currentCount = om.getOmRaftGroupManager()
          .getBucketRaftGroupAssignmentCount();
      int delta = currentCount - baselineCounts[i];
      assertEquals(1, delta,
          "BucketRaftGroupAssign should be applied exactly once on OM "
              + om.getOMNodeId() + " (baseline=" + baselineCounts[i]
              + ", current=" + currentCount + ")");
    }

    // All OMs must agree on the same raft group for the bucket
    UUID expectedRaftGroup = null;
    for (int i = 0; i < NUM_OF_OMS; i++) {
      OzoneManager om = cluster.getOzoneManager(i);
      Map<String, UUID> bucketRaftGroups =
          om.getOmRaftGroupManager().getBucketRaftGroups();
      UUID assignedGroup = bucketRaftGroups.get(bucketKey);
      assertNotNull(assignedGroup,
          "Bucket should have a raft group on OM " + om.getOMNodeId());

      if (expectedRaftGroup == null) {
        expectedRaftGroup = assignedGroup;
      } else {
        assertEquals(expectedRaftGroup, assignedGroup,
            "All OMs should agree on the same raft group. OM "
                + om.getOMNodeId() + " differs");
      }
    }
  }

  /**
   * The bucket raft groups survive OM restarts: they keep their ids (serials), logs and bucket assignments, and the
   * object IDs their transactions generated do not collide with the IDs generated after the restart.
   */
  @Test
  void testBucketRaftGroupsSurviveOmRestart() throws Exception {
    final int expectedRaftGroupsCount = 1 + MULTI_RAFT_BUCKET_GROUPS;
    waitForBucketRaftGroups(expectedRaftGroupsCount);
    final String volumeName = "restartvol";
    final String bucketName = "restartbucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    writeKey(volumeName, bucketName, "before-restart");

    final OzoneManager leader = cluster.getOMLeader();
    final String bucketKey = leader.getMetadataManager().getBucketKey(volumeName, bucketName);
    final Set<RaftGroupId> groupsBefore = new HashSet<>(leader.getOmRaftGroups().keySet());
    final UUID bucketGroup = leader.getOmRaftGroupManager().getBucketRaftGroups().get(bucketKey);
    assertNotNull(bucketGroup);
    assertTrue(OmRaftGroupManager.getBucketRaftGroupSerial(bucketGroup) > 0);
    final long objectIdBefore = getKeyObjectId(volumeName, bucketName, "before-restart");
    // the key was created by the bucket raft group: its object ID is above every OM raft group object ID
    assertTrue(objectIdBefore > OmUtils.getObjectIdFromTxId(OmUtils.EPOCH_WHEN_RATIS_ENABLED,
        OmRaftGroupManager.MAX_BUCKET_RAFT_GROUP_INDEX));

    cluster.restartOzoneManager();
    cluster.waitForLeaderOM();
    waitForBucketRaftGroups(expectedRaftGroupsCount);

    for (int i = 0; i < NUM_OF_OMS; i++) {
      final OzoneManager om = cluster.getOzoneManager(i);
      assertEquals(groupsBefore, om.getOmRaftGroups().keySet(), "raft groups of " + om.getOMNodeId());
      assertEquals(bucketGroup, om.getOmRaftGroupManager().getBucketRaftGroups().get(bucketKey),
          "raft group of the bucket on " + om.getOMNodeId());
      // the bucket raft group continues from its log, it is not re-created from index 0
      assertTrue(om.getTransactionInfo(RaftGroupId.valueOf(bucketGroup)).getTransactionIndex() > 0);
    }

    waitFor(() -> {
      try {
        writeKey(volumeName, bucketName, "after-restart");
        return true;
      } catch (IOException e) {
        LOG.info("Write after restart failed, retrying: {}", e.getMessage());
        return false;
      }
    }, 1000, 120_000);
    assertNotEquals(objectIdBefore, getKeyObjectId(volumeName, bucketName, "after-restart"));
    assertEquals("before-restart".length(),
        objectStore.getVolume(volumeName).getBucket(bucketName).getKey("before-restart").getDataSize());
  }

  private static void waitForBucketRaftGroups(int expectedRaftGroupsCount) throws Exception {
    waitFor(() -> {
      for (int i = 0; i < NUM_OF_OMS; i++) {
        if (cluster.getOzoneManager(i).getOmRaftGroups().size() != expectedRaftGroupsCount) {
          return false;
        }
      }
      return true;
    }, 1000, 120_000);
  }

  private static void writeKey(String volumeName, String bucketName, String keyName) throws IOException {
    try (OzoneOutputStream stream = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, keyName.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
      stream.write(keyName.getBytes(UTF_8));
    }
  }

  /**
   * The object ID of the key in the DB of the OM raft group leader. The leader may be a follower of the bucket raft
   * group that wrote the key, and apply the write a little later.
   */
  private static long getKeyObjectId(String volumeName, String bucketName, String keyName) throws Exception {
    final AtomicReference<OmKeyInfo> keyInfo = new AtomicReference<>();
    waitFor(() -> {
      final OzoneManager leader = cluster.getOMLeader();
      try {
        keyInfo.set(leader == null ? null : leader.getMetadataManager().getKeyTable(BucketLayout.OBJECT_STORE)
            .get(leader.getMetadataManager().getOzoneKey(volumeName, bucketName, keyName)));
      } catch (IOException e) {
        return false;
      }
      return keyInfo.get() != null;
    }, 100, 30_000);
    return keyInfo.get().getObjectID();
  }

}
