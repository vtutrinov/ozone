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

package org.apache.hadoop.ozone.om.multiraft;

import static org.apache.hadoop.ozone.om.OmRaftGroupManager.generateRaftGroups;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.BucketRaftGroupsStateChanged;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background task that runs on the Ozone Manager leader to reconcile the
 * state of bucket Raft groups. It removes the Raft groups that are closed, and creates new Raft groups (with new
 * serials) if the number of existing groups is less than the expected count. Groups without a leader or with
 * unhealthy peers are kept: they recover from their logs (SDP: the bucket raft groups survive OM restarts).
 */
public class BucketRaftGroupReconciliationTask implements BackgroundTask {

  public static final Logger LOG = LoggerFactory.getLogger(BucketRaftGroupReconciliationTask.class);

  private final OzoneManager ozoneManager;
  private final int expectedRaftGroupsCount;

  public BucketRaftGroupReconciliationTask(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    this.expectedRaftGroupsCount = ozoneManager.getOmRaftGroupManager().getOmRaftGroupCount();
  }

  @Override
  public BackgroundTaskResult call() throws Exception {
    // SDPOZN-2164: the leadership balancer does not move leaders while the bucket raft groups are reconstructed
    ozoneManager.getOmRaftGroupManager().acquireBucketRaftGroupsReconstructionLock();
    try {
      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftGroup mainRaftGroup = omRatisServer.getCurrentRaftGroup();
      long currentMultiRaftTerm = ozoneManager.getBucketRaftGroupsReconfigurationIndex();
      if (!omRatisServer.checkLeaderStatus(mainRaftGroup.getGroupId()).equals(NOT_LEADER)) {
        LOG.trace("Start reconciling bucket group RaftGroup on leader {}", omRatisServer.getRaftPeerId());
        List<RaftGroup> groupsToBeReconfigured = new ArrayList<>();
        List<RaftGroup> existingRaftGroups = (List<RaftGroup>) omRatisServer.getServer().getGroups();
        final long maxSerial = ozoneManager.getOmRaftGroupManager().getMaxBucketRaftGroupSerial();
        if (existingRaftGroups.size() == 1) { // consist of only main raft group, as like as an initial setup
          LOG.trace("Create all raft groups");
          List<RaftGroupId> raftGroupIds = generateRaftGroups(maxSerial, expectedRaftGroupsCount);
          ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()), true);
        } else {
          for (RaftGroup raftGroup : existingRaftGroups) {
            if (raftGroup.getGroupId().equals(mainRaftGroup.getGroupId())) {
              continue;
            }
            RaftGroupId groupId = raftGroup.getGroupId();
            DivisionInfo divisionInfo = omRatisServer.getServer().getDivision(groupId).getInfo();
            RaftPeerId leaderId = divisionInfo.getLeaderId();
            if (divisionInfo.getLifeCycleState().equals(LifeCycle.State.CLOSED)) {
              LOG.warn("Raft group {} is closed, removing it.", groupId);
              groupsToBeReconfigured.add(raftGroup);
            } else if (leaderId == null) {
              // e.g. electing a leader after a restart: the group recovers from its log, removing it would lose
              // the transactions not applied yet
              LOG.info("Raft group {} has no leader yet.", groupId);
            } else {
              RaftPeerId raftGroupLeaderId = omRatisServer.getServer().getDivision(groupId).getInfo().getLeaderId();

              try {
                final OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest healthStateRequest =
                    OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest.newBuilder()
                        .setGroupId(HddsUtils.toProtobuf(groupId.getUuid()))
                        .build();
                final OzoneManagerProtocolProtos.GetRaftGroupHealthStateResponse raftGroupHealthState;
                if (omRatisServer.getServer().getId().equals(raftGroupLeaderId)) {
                  // the current OM is the leader of the raft group that we are checking,
                  // there is no need to call the remote leader OM of the RAFT group
                  raftGroupHealthState = ozoneManager.getRaftGroupHealthState(healthStateRequest);
                } else {
                  // directly to the leader of the raft group, which answers the request (not through an OM client,
                  // which would start with the OM raft group leader lookup and fail over to the group leader)
                  raftGroupHealthState = ozoneManager.getOmRaftGroupManager().submitToOm(raftGroupLeaderId.toString(),
                      OzoneManagerProtocolProtos.OMRequest.newBuilder()
                          .setCmdType(OzoneManagerProtocolProtos.Type.GetRaftGroupHealthState)
                          .setClientId(omRatisServer.getCurrentClientId().toString())
                          .setGetRaftGroupHealthStateRequest(healthStateRequest)
                          .build())
                      .getGetRaftGroupHealthStateResponse();
                }
                boolean isNotHealthy = raftGroupHealthState.getPeerHealthInfoList()
                    .stream()
                    .anyMatch(it -> !it.getIsHealthy());
                if (isNotHealthy) {
                  // a lagging or stopped peer catches up from the log (or a snapshot) of the group when it is back;
                  // the group commits with a majority meanwhile
                  LOG.warn("Raft group {} has unhealthy peers: {}", groupId,
                      raftGroupHealthState.getPeerHealthInfoList());
                }
              } catch (Exception e) {
                LOG.warn("Failed to get raft group health state for group {}: {}",
                    groupId, e.getMessage());
              }
            }
          }
          groupsToBeReconfigured.forEach(this::deleteRaftGroup);

          if (ozoneManager.isMultiRaftEnabled()) {
            LOG.trace("Raft group to be reconfigured: {}", groupsToBeReconfigured);
            if (!groupsToBeReconfigured.isEmpty()) {
              ozoneManager.moveOmToSafeMode();
            }
            // removed groups are replaced by new ones, under new ids (serials)
            final int remaining = existingRaftGroups.size() - 1 - groupsToBeReconfigured.size();
            final int missing = expectedRaftGroupsCount - remaining;
            if (missing > 0) {
              List<RaftGroupId> raftGroupIds = generateRaftGroups(maxSerial, missing);
              LOG.trace("Raft group to be created: {}", raftGroupIds);
              ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()),
                  false);
            }
          }
        }
        boolean raftGroupsReconfigured = existingRaftGroups.size() == 1 || !groupsToBeReconfigured.isEmpty() ||
            existingRaftGroups.size() < expectedRaftGroupsCount + 1;
        if (raftGroupsReconfigured) {
          try {
            byte[] clientId = omRatisServer.getCurrentClientId().toByteString().toByteArray();
            Server.Call fakeCall = new Server.Call(
                (int) OzoneManagerRatisServer.nextCallId(),
                0,
                null,
                null,
                RPC.RpcKind.RPC_BUILTIN,
                clientId
            );
            RPC.Server.getCurCall().set(fakeCall);
            OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest bucketRaftGroupsStateChangedRequest =
                OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest.newBuilder()
                    .setStateChangedIndex(currentMultiRaftTerm + 1)
                    .build();
            OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setBucketRaftGroupsStateChangedRequest(bucketRaftGroupsStateChangedRequest)
                .setCmdType(BucketRaftGroupsStateChanged)
                .setClientId(omRatisServer.getCurrentClientId().toString())
                .build();
            omRatisServer.submitRequest(omRequest, true);
          } finally {
            RPC.Server.getCurCall().remove();
          }
        }
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    } finally {
      ozoneManager.getOmRaftGroupManager().releaseBucketRaftGroupsReconstructionLock();
    }
  }

  private void deleteRaftGroup(RaftGroup raftGroup) {
    String rpcType = ozoneManager.getConfiguration()
        .get(ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneManager.getConfiguration());

    GrpcTlsConfig tlsConfig = null;
    if (ozoneManager.isSecurityEnabled()) {
      try {
        tlsConfig = new GrpcTlsConfig(ozoneManager.getCertificateClient().getKeyManager(),
            ozoneManager.getCertificateClient().getTrustManager(), true);
      } catch (CertificateException ex) {
        LOG.error("Can't retrieve cert key store factory", ex);
      }
    }
    LOG.trace("{} Delete raft group {}.", ozoneManager.getOMNodeId(), raftGroup.getGroupId());
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    GrpcTlsConfig finalTlsConfig = tlsConfig;
    raftGroup.getPeers().stream()
        .filter(peer -> !peer.getId().equals(omRatisServer.getRaftPeerId()))
        .forEach(peer -> {
          try (RaftClient raftClient = RatisHelper.newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType),
              peer, retryPolicy, finalTlsConfig, ozoneManager.getConfiguration())) {
            raftClient.getGroupManagementApi(peer.getId()).remove(raftGroup.getGroupId(), true, false);
          } catch (IOException e) {
            LOG.error("An error occurred on deleting raft group {} remotely from {}",
                raftGroup.getGroupId(), peer.getId());
          }
        });
    try {
      omRatisServer.removeBucketRaftGroup(raftGroup.getGroupId());
    } catch (IOException e) {
      LOG.error("An error occurred on deleting raft group {} from {}", raftGroup.getGroupId(),
          omRatisServer.getRaftPeerId());
    }

  }
}
