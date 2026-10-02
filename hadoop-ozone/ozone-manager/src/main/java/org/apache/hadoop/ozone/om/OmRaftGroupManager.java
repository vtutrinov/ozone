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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_ACQUIRE_RETRY_SLEEP_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_ACQUIRE_RETRY_SLEEP_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_MAX_AWAIT_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_MAX_AWAIT_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRaftGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRaftGroupManager.class);

  private static final long ASSIGNMENT_TIMEOUT_MS = 30_000;

  /**
   * Bucket raft group ids carry a serial, unique over the lifetime of the cluster: a new group always gets a serial
   * above every earlier one (the high-water mark is replicated through the main raft group), so a group is never
   * re-created with a log starting from 0 under an id used before. The serial also forms the high bits of the object
   * and update IDs the group's transactions generate (see {@link #toExecutionIndex}).
   */
  private static final long BUCKET_RAFT_GROUP_UUID_MSB = 0x5d9b0c4e7a1d4b2eL;
  /** Bits of a bucket raft group log index in the execution index, below the serial. */
  public static final int BUCKET_RAFT_GROUP_INDEX_BITS = WithObjectID.RAFT_GROUP_SERIAL_SHIFT;
  public static final long MAX_BUCKET_RAFT_GROUP_INDEX = (1L << BUCKET_RAFT_GROUP_INDEX_BITS) - 1;
  /** Serials 1..1022: the execution index stays within OmUtils.MAX_TRXN_ID (2^54 - 2). */
  public static final long MAX_BUCKET_RAFT_GROUP_SERIAL = (1L << (54 - BUCKET_RAFT_GROUP_INDEX_BITS)) - 2;
  /** multiRaftInfoTable key of the highest bucket raft group serial allocated. */
  public static final String MAX_BUCKET_RAFT_GROUP_SERIAL_KEY = "maxBucketRaftGroupSerial";

  private final int omRaftGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;
  private final OzoneManager ozoneManager;

  private final Map<String, UUID> bucketRaftGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> bucketsPerRaftGroupCounter = new ConcurrentHashMap<>();

  // SDPOZN-2164: held by the raft group reconciliation task, so that the leadership balancer does not run meanwhile
  private final ReentrantReadWriteLock raftGroupsReconstructionLock = new ReentrantReadWriteLock();

  // raft group assignments in progress on this OM, by bucket: concurrent writers of a bucket wait for the first one
  private final Map<String, CompletableFuture<RaftGroupId>> bucketAssignments = new ConcurrentHashMap<>();
  // number of BucketRaftGroupAssign requests applied by this OM (tests)
  private final AtomicInteger bucketRaftGroupAssignmentCount = new AtomicInteger(0);
  // the cluster wide assignment lock, set and reset through the main raft group on every OM
  private final AtomicBoolean bucketRaftGroupAssignmentInProgress = new AtomicBoolean(false);

  // transports to the main raft group leader, by OM node id
  private final Map<String, OmTransport> omTransportCache = new ConcurrentHashMap<>();
  private final ExecutorService transportCreationExecutor;
  private final long assignmentLockMaxAwaitTime;
  private final long assignmentLockRetrySleepTime;

  public OmRaftGroupManager(
      OzoneManager ozoneManager,
      OzoneConfiguration configuration,
      boolean multiRaftEnabled,
      String omServiceId,
      OMMetadataManager metadataManager
  ) {
    omRaftGroupCount = configuration.getPositiveIntOrDefault(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServiceId = omServiceId;
    this.metadataManager = metadataManager;
    this.ozoneManager = ozoneManager;
    this.assignmentLockMaxAwaitTime = configuration.getLong(OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_MAX_AWAIT_TIME,
        OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_MAX_AWAIT_TIME_DEFAULT);
    this.assignmentLockRetrySleepTime = configuration.getInt(
        OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_ACQUIRE_RETRY_SLEEP_TIME,
        OZONE_OM_BUCKET_RAFT_GROUP_ASSIGNMENT_LOCK_ACQUIRE_RETRY_SLEEP_TIME_DEFAULT);
    // transports are created outside of the IPC handler threads
    this.transportCreationExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("OmTransport-Creator-%d")
        .setDaemon(true)
        .build());
  }

  public void close() {
    for (OmTransport transport : omTransportCache.values()) {
      try {
        transport.close();
      } catch (IOException e) {
        LOG.warn("Failed to close OM transport", e);
      }
    }
    omTransportCache.clear();
    transportCreationExecutor.shutdownNow();
  }

  public void reset() {
    bucketRaftGroupAssignmentCount.set(0);
    bucketRaftGroups.clear();
    bucketsPerRaftGroupCounter.clear();
  }

  /**
   * Restores a bucket raft group this OM recovered from its Ratis storage on start, and the assignment of the
   * buckets to it (persisted in the bucket info). A bucket assigned to a group that no longer exists is assigned
   * again on its next write.
   */
  public void restoreRaftGroup(RaftGroupId bucketRaftGroupId) {
    if (!multiRaftEnabled) {
      return;
    }
    final UUID raftGroup = bucketRaftGroupId.getUuid();
    bucketsPerRaftGroupCounter.putIfAbsent(raftGroup, 0);
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator =
            metadataManager.getBucketIterator();
    while (bucketIterator.hasNext()) {
      OmBucketInfo bucketInfo = bucketIterator.next().getValue().getCacheValue();
      if (bucketInfo != null && raftGroup.equals(bucketInfo.getRaftGroup())) {
        String key = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
        if (bucketRaftGroups.putIfAbsent(key, raftGroup) == null) {
          bucketsPerRaftGroupCounter.computeIfPresent(raftGroup, (k, v) -> v + 1);
        }
      }
    }
  }

  /**
   * Applies a bucket to raft group assignment (BucketRaftGroupAssign, replicated through the main raft group).
   * The first assignment of a bucket wins.
   * @return the raft group of the bucket
   */
  public UUID defineRaftGroupForBucket(String bucketPath, UUID raftGroupUUID) {
    bucketRaftGroupAssignmentCount.incrementAndGet();
    UUID existing = bucketRaftGroups.putIfAbsent(bucketPath, raftGroupUUID);
    if (existing != null) {
      return existing;
    }
    incrRaftGroupUsageCounter(RaftGroupId.valueOf(raftGroupUUID));
    return raftGroupUUID;
  }

  /**
   * Counts the transactions applied by a bucket raft group: the least used group gets the next bucket.
   */
  public void incrRaftGroupUsageCounter(RaftGroupId raftGroupId) {
    bucketsPerRaftGroupCounter.computeIfPresent(raftGroupId.getUuid(), (k, v) -> v + 1);
  }

  /**
   * Returns the raft group handling the write requests of the bucket. A bucket without a group is assigned the least
   * used one through the main raft group, so that all OMs agree on the assignment.
   */
  public RaftGroupId getRaftGroupToHandleBucketWriteRequest(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return RaftGroupId.valueOf(toUuid(omServiceId));
    }
    String bucketPath = metadataManager.getBucketKey(volumeName, bucketName);
    UUID raftGroup = bucketRaftGroups.get(bucketPath);
    return raftGroup != null ? RaftGroupId.valueOf(raftGroup) : assignRaftGroup(bucketPath);
  }

  private RaftGroupId assignRaftGroup(String bucketPath) {
    final CompletableFuture<RaftGroupId> myAssignment = new CompletableFuture<>();
    final CompletableFuture<RaftGroupId> activeAssignment = bucketAssignments.putIfAbsent(bucketPath, myAssignment);
    if (activeAssignment != null) {
      // another handler thread is assigning this bucket
      try {
        return activeAssignment.get(assignmentLockMaxAwaitTime + ASSIGNMENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Raft group assignment for bucket " + bucketPath + " failed", e);
      }
    }

    try {
      // the assignment may have completed between the caller's lookup and putIfAbsent
      UUID raftGroup = bucketRaftGroups.get(bucketPath);
      if (raftGroup == null) {
        awaitBucketRaftGroupsInitialization();
        try {
          acquireBucketRaftGroupAssignmentLock();
          // the bucket may have been assigned by another OM while waiting for the lock
          raftGroup = bucketRaftGroups.get(bucketPath);
          if (raftGroup == null) {
            UUID lessLoadedRaftGroup = selectLessLoadedRaftGroup();
            LOG.info("Raft group {} selected to handle write requests to {}", lessLoadedRaftGroup, bucketPath);
            // the main group leader returns the effective group: this OM may not have applied the assignment yet
            raftGroup = assignRaftGroupToBucket(bucketPath, lessLoadedRaftGroup);
          }
        } finally {
          // released even if acquiring failed: the outcome of a failed acquire request is unknown, and a lock left
          // behind would block all assignments; the assignment itself is first-wins, so a spurious release is safe.
          // A lock left behind by a failed release is released by the next assignment after its acquire timeout.
          try {
            releaseBucketRaftGroupAssignmentWriteLockByRaft();
          } catch (IOException e) {
            LOG.warn("Failed to release the bucket raft group assignment lock", e);
          }
        }
      }
      RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroup);
      myAssignment.complete(raftGroupId);
      return raftGroupId;
    } catch (IOException e) {
      IllegalStateException ex = new IllegalStateException("Failed to assign a raft group to bucket " + bucketPath, e);
      myAssignment.completeExceptionally(ex);
      throw ex;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IllegalStateException ex = new IllegalStateException(e);
      myAssignment.completeExceptionally(ex);
      throw ex;
    } catch (RuntimeException e) {
      myAssignment.completeExceptionally(e);
      throw e;
    } finally {
      bucketAssignments.remove(bucketPath, myAssignment);
    }
  }

  private void acquireBucketRaftGroupAssignmentLock() throws IOException, InterruptedException {
    long deadline = Time.monotonicNow() + assignmentLockMaxAwaitTime;
    while (!acquireBucketRaftGroupAssignmentWriteLockByRaft()) {
      if (Time.monotonicNow() > deadline) {
        throw new IOException("Timed out acquiring bucket raft group assignment lock");
      }
      LOG.debug("Waiting for bucket raft group assignment write lock to be released");
      Thread.sleep(assignmentLockRetrySleepTime);
    }
  }

  @VisibleForTesting
  boolean acquireBucketRaftGroupAssignmentWriteLockByRaft() throws IOException {
    return submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.AcquireBucketRaftGroupAssignmentWriteLock)
        .setClientId(ClientId.randomId().toString())
        .build()).getSuccess();
  }

  @VisibleForTesting
  void releaseBucketRaftGroupAssignmentWriteLockByRaft() throws IOException {
    submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.ReleaseBucketRaftGroupAssignmentWriteLock)
        .setClientId(ClientId.randomId().toString())
        .build());
  }

  /**
   * Assigns the raft group to the bucket through the main raft group, so that all OMs agree on the mapping.
   * @return the raft group of the bucket (an earlier assignment wins)
   */
  @VisibleForTesting
  UUID assignRaftGroupToBucket(String bucketPath, UUID raftGroupUUID) throws IOException {
    return HddsUtils.fromProtobuf(submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.BucketRaftGroupAssign)
        .setBucketRaftGroupAssignRequest(BucketRaftGroupAssignRequest.newBuilder()
            .setBucketPath(bucketPath)
            .setRaftGroupId(HddsUtils.toProtobuf(raftGroupUUID)))
        .setClientId(ozoneManager.getOmRatisServer().getCurrentClientId().toString())
        .build()).getBucketRaftGroupAssignResponse().getRaftGroupId());
  }

  private OMResponse submitToMainGroupLeader(OMRequest request) throws IOException {
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    RaftPeerId leaderPeerId = omRatisServer.getServer()
        .getDivision(omRatisServer.getRaftGroupId()).getInfo().getLeaderId();
    if (leaderPeerId == null) {
      throw new IOException("The main OM raft group has no leader");
    }
    String omServiceIdOfCluster = OmUtils.getOzoneManagerServiceId(ozoneManager.getConfiguration());
    OMNodeDetails leader = OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceIdOfCluster, true)
        .stream()
        .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(leaderPeerId.toString()))
        .findFirst()
        .orElseThrow(() -> new IOException("Unknown main OM raft group leader " + leaderPeerId));
    OMResponse response = getOrCreateOmTransport(leader.getNodeId()).submitRequest(request, leader.getNodeId());
    if (response.getStatus() != OzoneManagerProtocolProtos.Status.OK) {
      throw new IOException(request.getCmdType() + " failed with " + response.getStatus() + ": "
          + response.getMessage());
    }
    return response;
  }

  private OmTransport getOrCreateOmTransport(String omNodeId) throws IOException {
    OmTransport transport = omTransportCache.get(omNodeId);
    if (transport != null) {
      return transport;
    }
    synchronized (omTransportCache) {
      transport = omTransportCache.get(omNodeId);
      if (transport != null) {
        return transport;
      }
      CompletableFuture<OmTransport> futureTransport = CompletableFuture.supplyAsync(() -> {
        try {
          return UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<OmTransport>) () -> {
            if (ozoneManager.getCertificateClient() != null) {
              GrpcOmTransport.setCaCerts(ozoneManager.getCertificateClient().getTrustChain());
            }
            return new GrpcOmTransport(ozoneManager.getConfiguration(), UserGroupInformation.getLoginUser(),
                omServiceId);
          });
        } catch (IOException | InterruptedException e) {
          throw new CompletionException(e);
        }
      }, transportCreationExecutor);
      try {
        transport = futureTransport.get(ASSIGNMENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while creating OmTransport for " + omNodeId, e);
      } catch (TimeoutException e) {
        throw new IOException("Timeout while creating OmTransport for " + omNodeId, e);
      } catch (ExecutionException e) {
        throw new IOException("Failed to create OmTransport for " + omNodeId, e.getCause());
      }
      omTransportCache.put(omNodeId, transport);
      return transport;
    }
  }

  @VisibleForTesting
  void awaitBucketRaftGroupsInitialization() throws InterruptedException {
    while (bucketsPerRaftGroupCounter.size() < omRaftGroupCount) {
      LOG.info("Waiting for group initiating {}-{}. {}", bucketsPerRaftGroupCounter.size(), omRaftGroupCount,
          bucketsPerRaftGroupCounter);
      Thread.sleep(1000);
    }
  }

  private UUID selectLessLoadedRaftGroup() {
    return bucketsPerRaftGroupCounter.entrySet().stream()
        .min(Comparator.comparingInt(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .get();
  }

  /** Applied through the main raft group (AcquireBucketRaftGroupAssignmentWriteLock). */
  public boolean acquireBucketRaftGroupAssignmentWriteLock() {
    return bucketRaftGroupAssignmentInProgress.compareAndSet(false, true);
  }

  /** Applied through the main raft group (ReleaseBucketRaftGroupAssignmentWriteLock). */
  public void releaseBucketRaftGroupAssignmentWriteLock() {
    LOG.info("Bucket raft group assignment write lock released");
    bucketRaftGroupAssignmentInProgress.set(false);
  }

  public void acquireBucketRaftGroupsReconstructionLock() {
    raftGroupsReconstructionLock.writeLock().lock();
  }

  public void releaseBucketRaftGroupsReconstructionLock() {
    raftGroupsReconstructionLock.writeLock().unlock();
  }

  @VisibleForTesting
  Map<String, CompletableFuture<RaftGroupId>> getBucketAssignmentLocks() {
    return bucketAssignments;
  }

  public int getBucketRaftGroupAssignmentCount() {
    return bucketRaftGroupAssignmentCount.get();
  }

  public Map<String, UUID> getBucketRaftGroups() {
    return bucketRaftGroups;
  }

  public int getOmRaftGroupCount() {
    return omRaftGroupCount;
  }

  /** Ids of {@code count} new bucket raft groups, with the serials following {@code maxSerial}. */
  public static List<RaftGroupId> generateRaftGroups(long maxSerial, int count) {
    List<RaftGroupId> result = new ArrayList<>(count);
    for (long serial = maxSerial + 1; serial <= maxSerial + count; serial++) {
      result.add(bucketRaftGroupId(serial));
    }
    return result;
  }

  public static RaftGroupId bucketRaftGroupId(long serial) {
    Preconditions.checkArgument(serial > 0 && serial <= MAX_BUCKET_RAFT_GROUP_SERIAL,
        "Bucket raft group serial %s out of range 1..%s", serial, MAX_BUCKET_RAFT_GROUP_SERIAL);
    return RaftGroupId.valueOf(new UUID(BUCKET_RAFT_GROUP_UUID_MSB, serial));
  }

  /** @return the serial of a bucket raft group id, or -1 if it is not one (e.g. the main OM raft group). */
  public static long getBucketRaftGroupSerial(UUID raftGroupUuid) {
    final long serial = raftGroupUuid.getLeastSignificantBits();
    return raftGroupUuid.getMostSignificantBits() == BUCKET_RAFT_GROUP_UUID_MSB
        && serial > 0 && serial <= MAX_BUCKET_RAFT_GROUP_SERIAL ? serial : -1;
  }

  /**
   * The index a transaction of the given bucket raft group executes with, i.e. the base of the object and update IDs
   * it generates: the group serial above the log index, so that the groups, and the OM raft group (serial 0, whose
   * indexes are used as they are), never generate the same IDs.
   */
  public static long toExecutionIndex(long serial, long logIndex) {
    Preconditions.checkState(logIndex <= MAX_BUCKET_RAFT_GROUP_INDEX,
        "Log index %s of the bucket raft group with serial %s exceeds %s", logIndex, serial,
        MAX_BUCKET_RAFT_GROUP_INDEX);
    return (serial << BUCKET_RAFT_GROUP_INDEX_BITS) | logIndex;
  }

  /** @return the highest bucket raft group serial allocated, as applied by the main raft group on this OM. */
  public long getMaxBucketRaftGroupSerial() throws IOException {
    final Long max = metadataManager.getMultiRaftInfoTable().get(MAX_BUCKET_RAFT_GROUP_SERIAL_KEY);
    return max == null ? 0 : max;
  }

  public static UUID toUuid(String groupId) {
    return UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public RaftGroupId addGroupIdToRaftGroupCounter(UUID groupUuid) {
    bucketsPerRaftGroupCounter.put(groupUuid, 0);
    return RaftGroupId.valueOf(groupUuid);
  }

  public void removeGroup(RaftGroupId raftGroupId) {
    bucketRaftGroups.entrySet().stream()
            .filter(e -> e.getValue().equals(raftGroupId.getUuid()))
            .map(Map.Entry::getKey)
            .forEach(bucketRaftGroups::remove);
    bucketsPerRaftGroupCounter.remove(raftGroupId.getUuid());
  }

}
