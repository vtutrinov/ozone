package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMCreateRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRaftGroupsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OMCreateRaftGroupsRequest extends OMClientRequest {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMCreateRaftGroupsRequest.class);

    public OMCreateRaftGroupsRequest(OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long transactionLogIndex) {

        OMRequest omRequest = getOmRequest();
        CreateBucketRaftGroupsRequest createBucketRaftGroupsRequest = omRequest.getCreateBucketRaftGroupsRequest();
        createBucketRaftGroupsRequest.getGroupIdsList().forEach(groupId -> {
            ozoneManager.createRaftGroupForBucket(RaftGroupId.valueOf(HddsUtils.fromProtobuf(groupId)));
            ozoneManager.getOmRatisGroupManager().incrementRatisGroupCounter(HddsUtils.fromProtobuf(groupId));
        });
        final OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
                OmResponseUtil.getOMResponseBuilder(omRequest);
        OzoneManagerProtocolProtos.CreateBucketRaftGroupsResponse createBucketRaftGroupsResponse = OzoneManagerProtocolProtos.CreateBucketRaftGroupsResponse.newBuilder().build();
        omResponse.setCreateBucketRaftGroupsResponse(createBucketRaftGroupsResponse);
        return new OMCreateRaftGroupsResponse(omResponse.build());
    }


}
