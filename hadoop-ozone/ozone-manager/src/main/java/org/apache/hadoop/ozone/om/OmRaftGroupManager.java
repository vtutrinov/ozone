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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

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
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
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

  private final int omRaftGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;
  private final OzoneManager ozoneManager;

  private final Map<String, UUID> bucketRaftGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> bucketsPerRaftGroupCounter = new ConcurrentHashMap<>();

  // raft group assignments in progress on this OM, by bucket: concurrent writers of a bucket wait for the first one
  private final Map<String, CompletableFuture<RaftGroupId>> bucketAssignments = new ConcurrentHashMap<>();
  // the cluster wide assignment lock, set and reset through the main raft group on every OM
  private final AtomicBoolean bucketRaftGroupAssignmentInProgress = new AtomicBoolean(false);

  // transports to the main raft group leader, by OM node id
  private final Map<String, OmTransport> omTransportCache = new ConcurrentHashMap<>();
  private final ExecutorService transportCreationExecutor;

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
    bucketRaftGroups.clear();
    bucketsPerRaftGroupCounter.clear();
  }

  private void initBucketMap() {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator =
            metadataManager.getBucketIterator();
    while (bucketIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo != null) {
        UUID raftGroup = bucketInfo.getRaftGroup();
        if (raftGroup != null) {
          String key = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
          bucketRaftGroups.put(key, raftGroup);
          bucketsPerRaftGroupCounter.compute(raftGroup, (k, v) -> v == null ? 1 : v + 1);
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
        return activeAssignment.get(ASSIGNMENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
        acquireBucketRaftGroupAssignmentWriteLockByRaft();
        try {
          // the bucket may have been assigned by another OM while waiting for the lock
          if (!bucketRaftGroups.containsKey(bucketPath)) {
            UUID lessLoadedRaftGroup = selectLessLoadedRaftGroup();
            LOG.info("Raft group {} selected to handle write requests to {}", lessLoadedRaftGroup, bucketPath);
            assignRaftGroupToBucket(bucketPath, lessLoadedRaftGroup);
          }
        } finally {
          releaseBucketRaftGroupAssignmentWriteLockByRaft();
        }
        raftGroup = bucketRaftGroups.get(bucketPath);
        if (raftGroup == null) {
          throw new IllegalStateException("Raft group assignment for bucket " + bucketPath + " is not applied yet");
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

  private void acquireBucketRaftGroupAssignmentWriteLockByRaft() throws IOException, InterruptedException {
    long deadline = Time.monotonicNow() + ASSIGNMENT_TIMEOUT_MS;
    while (!submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.AcquireBucketRaftGroupAssignmentWriteLock)
        .setClientId(ClientId.randomId().toString())
        .build()).getSuccess()) {
      if (Time.monotonicNow() > deadline) {
        throw new IOException("Timeout waiting for the bucket raft group assignment lock");
      }
      LOG.debug("Waiting for bucket raft group assignment write lock to be released");
      Thread.sleep(100);
    }
  }

  private void releaseBucketRaftGroupAssignmentWriteLockByRaft() throws IOException {
    submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.ReleaseBucketRaftGroupAssignmentWriteLock)
        .setClientId(ClientId.randomId().toString())
        .build());
  }

  /**
   * Assigns the raft group to the bucket through the main raft group, so that all OMs agree on the mapping.
   */
  private void assignRaftGroupToBucket(String bucketPath, UUID raftGroupUUID) throws IOException {
    submitToMainGroupLeader(OMRequest.newBuilder()
        .setCmdType(Type.BucketRaftGroupAssign)
        .setBucketRaftGroupAssignRequest(BucketRaftGroupAssignRequest.newBuilder()
            .setBucketPath(bucketPath)
            .setRaftGroupId(HddsUtils.toProtobuf(raftGroupUUID)))
        .setClientId(ozoneManager.getOmRatisServer().getCurrentClientId().toString())
        .build());
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

  private void awaitBucketRaftGroupsInitialization() throws InterruptedException {
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

  public Map<String, UUID> getBucketRaftGroups() {
    return bucketRaftGroups;
  }

  public int getOmRaftGroupCount() {
    return omRaftGroupCount;
  }

  public static List<RaftGroupId> generateRaftGroups(long currentTerm, int count) {
    List<RaftGroupId> result = new ArrayList<>(count);
    long startFrom = currentTerm * 100;
    for (long i = startFrom; i < startFrom + count; i++) {
      UUID raftGroupIdUUID = OmRaftGroupManager.toUuid(String.valueOf(i));
      RaftGroupId groupId = RaftGroupId.valueOf(raftGroupIdUUID);
      result.add(groupId);
    }
    return result;
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
