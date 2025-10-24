package org.apache.hadoop.ozone.om.response.bucket;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;

/**
 * Response for bucket raft group assignment.
 */
@CleanupTableInfo(cleanupTables = {BUCKET_TABLE})
public class OMBucketRaftGroupAssignResponse extends OMClientResponse {
  private OmBucketInfo omBucketInfo;

  public OMBucketRaftGroupAssignResponse(@Nonnull OMResponse omResponse,
                                         @Nullable OmBucketInfo bucketInfo) {
    super(omResponse);
    this.omBucketInfo = bucketInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getSuccess() && omBucketInfo != null) {
      String bucketKey = omMetadataManager.getBucketKey(
          omBucketInfo.getVolumeName(), omBucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          bucketKey, omBucketInfo);
    }
  }

}