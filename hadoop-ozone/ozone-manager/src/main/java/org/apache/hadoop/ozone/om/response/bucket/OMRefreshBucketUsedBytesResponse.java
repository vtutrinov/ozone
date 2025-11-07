package org.apache.hadoop.ozone.om.response.bucket;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;

/**
 * Response for RefreshUsedBytes request.
 */
@CleanupTableInfo(cleanupTables = {BUCKET_TABLE})
public class OMRefreshBucketUsedBytesResponse extends OMClientResponse {
  public OMRefreshBucketUsedBytesResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {

  }
}
