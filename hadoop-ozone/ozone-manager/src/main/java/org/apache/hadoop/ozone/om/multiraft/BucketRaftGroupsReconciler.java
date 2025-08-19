package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OmRaftGroupManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.BucketRaftGroupsStateChanged;

public class BucketRaftGroupsReconciler extends BackgroundService {

  private final OzoneManager ozoneManager;

  private final int expectedRaftGroupsCount;

  public BucketRaftGroupsReconciler(OzoneManager ozoneManager) {
    super("OMBucketRaftGroupsReconciler", ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL,
            OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS,
        1, 0, "OMBucketRaftGroupsReconciler-");
    this.ozoneManager = ozoneManager;
    this.expectedRaftGroupsCount = ozoneManager.getConfiguration().getInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
  }

  public class BucketRaftGroupReconciliationTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftGroup mainRaftGroup = omRatisServer.getCurrentRaftGroup();
      boolean raftGroupsReconfigured = false;
      Long currentMultiRaftTerm = ozoneManager.getMetadataManager().getMultiRaftInfoTable().get("term");
      if (currentMultiRaftTerm == null) {
        currentMultiRaftTerm = 0L;
      }
      if (!omRatisServer.checkLeaderStatus(mainRaftGroup.getGroupId()).equals(NOT_LEADER)) {
        List<RaftGroup> groupsToBeReconfigured = new ArrayList<>();
        List<RaftGroup> existingRaftGroups = (List<RaftGroup>) omRatisServer.getServer().getGroups();
        if (existingRaftGroups.size() == 1) { // consist of only main raft group, as like as an initial setup
          List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm, expectedRaftGroupsCount);
          ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()));
          raftGroupsReconfigured = true;
        } else {
          for (RaftGroup raftGroup : existingRaftGroups) {
            if (raftGroup.getGroupId().equals(mainRaftGroup.getGroupId())) continue;
            RaftGroupId groupId = raftGroup.getGroupId();
            DivisionInfo divisionInfo = omRatisServer.getServer().getDivision(groupId).getInfo();
            RaftPeerId leaderId = divisionInfo.getLeaderId();
            if (leaderId == null || divisionInfo.getLifeCycleState().equals(LifeCycle.State.CLOSED)) {
              LOG.warn("Raft group {} is closed, removing it.", groupId);
              groupsToBeReconfigured.add(raftGroup);
            } else {
              RaftPeerId raftGroupLeaderId = omRatisServer.getServer().getDivision(groupId).getInfo().getLeaderId();

              try (OzoneClient omClient = OzoneClientFactory.getRpcClient(raftGroupLeaderId.toString(),
                  OmUtils.getOmRpcPort(ozoneManager.getConfiguration()), ozoneManager.getConfiguration())) {
                UUID raftGroupUuid = groupId.getUuid();
                OzoneManagerProtocolProtos.GetRaftGroupHealthStateResponse raftGroupHealthState;
                if (omRatisServer.getServer().getId().equals(raftGroupLeaderId)) {
                  // the current OM is the leader of the raft group that we are checking, there is no need to call remote
                  // leader OM of the RAFT group
                  raftGroupHealthState = ozoneManager.getRaftGroupHealthState(
                      GetRaftGroupHealthStateRequest.newBuilder()
                          .setGroupId(HddsProtos.UUID.newBuilder()
                              .setLeastSigBits(raftGroupUuid.getLeastSignificantBits())
                              .setMostSigBits(raftGroupUuid.getMostSignificantBits())
                              .build())
                          .build());
                } else {
                  raftGroupHealthState =
                      omClient.getProxy().getRaftGroupHealthState(
                          GetRaftGroupHealthStateRequest.newBuilder()
                              .setGroupId(HddsProtos.UUID.newBuilder()
                                  .setLeastSigBits(raftGroupUuid.getLeastSignificantBits())
                                  .setMostSigBits(raftGroupUuid.getMostSignificantBits())
                                  .build())
                              .build());
                }
                AtomicBoolean isGroupHealthy = new AtomicBoolean(true);
                raftGroupHealthState.getPeerHealthInfoList().forEach(
                    peerHealthInfo -> {
                      if (!peerHealthInfo.getIsHealthy()) {
                        LOG.warn("Raft group {} peer {} is not healthy, removing it.",
                            groupId, peerHealthInfo.getPeerId());
                        isGroupHealthy.set(false);
                      }
                    });
                if (!isGroupHealthy.get()) {
                  groupsToBeReconfigured.add(raftGroup);
                }
              } catch (Exception e) {
                LOG.warn("Failed to get raft group health state for group {}: {}",
                    groupId, e.getMessage());
                groupsToBeReconfigured.add(raftGroup);
              }
            }
          }
          if (!groupsToBeReconfigured.isEmpty()) {
            groupsToBeReconfigured.forEach(BucketRaftGroupsReconciler.this::deleteRaftGroup);
            List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm + 1, groupsToBeReconfigured.size());
            ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()));
            raftGroupsReconfigured = true;
          }
          if (existingRaftGroups.size() < expectedRaftGroupsCount + 1) {
            List<RaftGroupId> ids = generateRaftGroups(currentMultiRaftTerm,
                expectedRaftGroupsCount - existingRaftGroups.size() + 1);
            ozoneManager.createRaftGroups(ids.stream().map(RaftId::getUuid).collect(Collectors.toList()));
            raftGroupsReconfigured = true;
          }
        }
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
            BucketRaftGroupsStateChangedRequest bucketRaftGroupsStateChangedRequest =
                BucketRaftGroupsStateChangedRequest.newBuilder()
                    .setStateChangedIndex(currentMultiRaftTerm + 1)
                    .build();
            OMRequest omRequest = OMRequest.newBuilder()
                .setBucketRaftGroupsStateChangedRequest(bucketRaftGroupsStateChangedRequest)
                .setCmdType(BucketRaftGroupsStateChanged)
                .setClientId(omRatisServer.getCurrentClientId().toString())
                .build();
            OMResponse omResponse = omRatisServer.submitRequest(omRequest);
          } finally {
            RPC.Server.getCurCall().remove();
          }
        }
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

  }

  private List<RaftGroupId> generateRaftGroups(long currentTerm, int count) {
    List<RaftGroupId> result = new ArrayList<>(count);
    long startFrom = currentTerm * 100;
    for (long i = startFrom; i < startFrom + count; i++) {
      UUID raftGroupIdUUID = OmRaftGroupManager.toUuid(String.valueOf(i));
      RaftGroupId groupId = RaftGroupId.valueOf(raftGroupIdUUID);
      result.add(groupId);
    }
    return result;
  }

  private void deleteRaftGroup(RaftGroup raftGroup) {
    String rpcType = ozoneManager.getConfiguration()
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneManager.getConfiguration());

    GrpcTlsConfig tlsConfig = null;
    if (ozoneManager.isSecurityEnabled()) {
      try {
        tlsConfig = new GrpcTlsConfig(ozoneManager.getCertificateClient().getClientKeyStoresFactory().getKeyManagers()[0],
            ozoneManager.getCertificateClient().getClientKeyStoresFactory().getTrustManagers()[0], true);
      } catch (IOException ex) {
        LOG.error("Can't retrieve cert key store factory");
      }
    }
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    GrpcTlsConfig finalTlsConfig = tlsConfig;
    raftGroup.getPeers().stream()
        .filter(pear -> !pear.getId().equals(omRatisServer.getRaftPeerId()))
        .forEach(peer -> {
          try (RaftClient raftClient = RatisHelper.newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType),
              peer, retryPolicy, finalTlsConfig, ozoneManager.getConfiguration())) {
            raftClient.getGroupManagementApi(peer.getId()).remove(raftGroup.getGroupId(), true, false);
          } catch (IOException e) {
            LOG.error("An error occurred on deleting raft group {} from {}", raftGroup.getGroupId(), peer.getId());
          }
        });
    try {
      omRatisServer.removeBucketRaftGroup(raftGroup.getGroupId());
    } catch (IOException e) {
      LOG.error("An error occurred on deleting raft group {} from {}", raftGroup.getGroupId(),
          omRatisServer.getRaftPeerId());
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue tasksQueue = new BackgroundTaskQueue();
    tasksQueue.add(new BucketRaftGroupReconciliationTask());
    return tasksQueue;
  }
}
