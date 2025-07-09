package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMCreateRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveBucketRaftGroupsResponse;

/**
 * Handles remove raft group request.
 */
public class OMRemoveRaftGroupsRequest extends OMClientRequest {
  private static final Logger LOG =
          LoggerFactory.getLogger(OMRemoveRaftGroupsRequest.class);

  public OMRemoveRaftGroupsRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long transactionLogIndex) {

    OMRequest omRequest = getOmRequest();
    OzoneManagerProtocolProtos.RemoveBucketRaftGroupsRequest removeBucketRaftGroupsRequest =
            omRequest.getRemoveBucketRaftGroupsRequest();

    ozoneManager.getOmRatisGroupManager().reset();
    removeBucketRaftGroupsRequest.getGroupIdsList().forEach(
            groupId -> {
              LOG.trace("Start removing RAFT group in {}: {}", ozoneManager.getOMNodeId(), groupId);
              ozoneManager.removeRaftGroupForBucket(RaftGroupId.valueOf(fromProtobuf(groupId)));
            }
    );
    final OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());
    RemoveBucketRaftGroupsResponse removeBucketRaftGroupsResponse = RemoveBucketRaftGroupsResponse
            .newBuilder()
            .build();
    omResponse.setRemoveBucketRaftGroupsResponse(removeBucketRaftGroupsResponse);
    return new OMCreateRaftGroupsResponse(omResponse.build());
  }
}
