package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.util.LifeCycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.BucketRaftGroupsStateChanged;

public class BucketRaftGroupsReconciler extends BackgroundService {

  private final OzoneManager ozoneManager;

  public BucketRaftGroupsReconciler(OzoneManager ozoneManager) {
    super("OMBucketRaftGroupsReconciler", ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL,
            OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS,
        1, 0, "OMBucketRaftGroupsReconciler-");
    this.ozoneManager = ozoneManager;
  }

  public class BucketRaftGroupReconciliationTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftGroup mainRaftGroup = omRatisServer.getCurrentRaftGroup();
      boolean raftGroupsReconfigured = false;
      if (!omRatisServer.checkLeaderStatus(mainRaftGroup.getGroupId()).equals(NOT_LEADER)) {
        Map<RaftGroupId, RaftConfigurationProto> expectedRaftGroupsConfiguration = new HashMap<>();
        try (TableIterator<RaftGroupId, ? extends Table.KeyValue<RaftGroupId, RaftConfigurationProto>>
                 raftGroupInfoIterator = ozoneManager.getMetadataManager().getRaftGroupConfigurationTable().iterator()) {
          while (raftGroupInfoIterator.hasNext()) {
            Table.KeyValue<RaftGroupId, RaftConfigurationProto> entry = raftGroupInfoIterator.next();
            RaftGroupId groupId = entry.getKey();
            RaftConfigurationProto configuration = entry.getValue();
            expectedRaftGroupsConfiguration.put(groupId, configuration);
          }
        }
        List<UUID> groupsToBeReconfigured = new ArrayList<>();
        for (RaftGroup raftGroup : omRatisServer.getServer().getGroups()) {
          if (raftGroup.getGroupId().equals(mainRaftGroup.getGroupId())) continue;
          RaftGroupId groupId = raftGroup.getGroupId();
          if (expectedRaftGroupsConfiguration.containsKey(groupId)) {

            DivisionInfo divisionInfo = omRatisServer.getServer().getDivision(groupId).getInfo();
            RaftPeerId leaderId = divisionInfo.getLeaderId();
            if (leaderId == null || divisionInfo.getLifeCycleState().equals(LifeCycle.State.CLOSED)) {
              LOG.warn("Raft group {} is closed, removing it.", groupId);
              groupsToBeReconfigured.add(groupId.getUuid());
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
                  groupsToBeReconfigured.add(raftGroupUuid);
                }
              } catch (Exception e) {
                LOG.warn("Failed to get raft group health state for group {}: {}",
                    groupId, e.getMessage());
                groupsToBeReconfigured.add(groupId.getUuid());
              }
            }
            expectedRaftGroupsConfiguration.remove(groupId);
          } else {
            LOG.warn("Raft group {} is not expected, removing it.", groupId);
            ozoneManager.removeRaftGroupForBucket(groupId);
          }
        }
        if (!groupsToBeReconfigured.isEmpty()) {
          groupsToBeReconfigured.forEach(raftGroupIdUUID -> {
            try {
              omRatisServer.getServer().setConfiguration(
                  new SetConfigurationRequest(omRatisServer.getCurrentClientId(), omRatisServer.getRaftPeerId(),
                      RaftGroupId.valueOf(raftGroupIdUUID), OzoneManagerRatisServer.nextCallId(),
                      Collections.singletonList(omRatisServer.getLeader())));
            } catch (IOException e) {
              LOG.error("An error occurred on changing raft group configuration fro raft group {}", raftGroupIdUUID, e);
            }
          });
          ozoneManager.removeRaftGroups(groupsToBeReconfigured);
          ozoneManager.createRaftGroups(groupsToBeReconfigured);
          raftGroupsReconfigured = true;
        }
        if (!expectedRaftGroupsConfiguration.isEmpty()) {
          List<UUID> ids = expectedRaftGroupsConfiguration.keySet().stream()
              .map(RaftGroupId::getUuid).collect(Collectors.toList());
          ozoneManager.createRaftGroups(ids);
          raftGroupsReconfigured = true;
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
                    .setStateChangedIndex(ozoneManager.getCurrentMultiRaftTerm() + 1)
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

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue tasksQueue = new BackgroundTaskQueue();
    tasksQueue.add(new BucketRaftGroupReconciliationTask());
    return tasksQueue;
  }
}
