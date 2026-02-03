package org.apache.hadoop.ozone.om.response.group;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@CleanupTableInfo
public class OMCreateRaftGroupsResponse extends OMClientResponse {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMCreateRaftGroupsResponse.class);

    public OMCreateRaftGroupsResponse(OMResponse omResponse) {
        super(omResponse);
    }


    @Override
    protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {

    }
}
