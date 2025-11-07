package org.apache.hadoop.ozone.om.request.bucket;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMRefreshBucketUsedBytesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RefreshBucketUsedBytesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RefreshBucketUsedBytesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;


/**
 * Handle RefreshBucketUsedBytes Request.
 */
public class OMRefreshBucketUsedBytesRequest extends OMClientRequest {

  public static final Logger LOG = LoggerFactory.getLogger(OMRefreshBucketUsedBytesRequest.class);

  public OMRefreshBucketUsedBytesRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long transactionLogIndex) {
    OMRequest omRequest = getOmRequest();
    RefreshBucketUsedBytesRequest refreshBucketUsedBytesRequest = omRequest.getRefreshBucketUsedBytesRequest();
    String volumeName = refreshBucketUsedBytesRequest.getVolumeName();
    String bucketName = refreshBucketUsedBytesRequest.getBucketName();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(omRequest);

    long totalDataSize = 0;
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OMLockDetails omLockDetails;
    boolean acquiredLock = false;
    try {
      OmBucketInfo bucketInfo = ozoneManager.getBucketInfo(volumeName, bucketName);
      if (bucketInfo == null) {
        throw new OMException("Bucket not found: " + volumeName + "/" + bucketName,
                OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      if (bucketInfo.getUsedBytes() == 0) {
        buildSuccessfulOMResponse(totalDataSize, omResponse);
        return new OMRefreshBucketUsedBytesResponse(omResponse.build());
      }

      BucketLayout layout = bucketInfo.getBucketLayout();
      Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(layout);
      List<OmKeyInfo> keys = new ArrayList<>();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iter = keyTable.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = iter.next();
          OmKeyInfo keyInfo = kv.getValue();

          if (keyInfo != null
                  && volumeName.equals(keyInfo.getVolumeName())
                  && bucketName.equals(keyInfo.getBucketName())) {
            keys.add(keyInfo);
          }
        }
      }

      for (OmKeyInfo key : keys) {
        if (key != null) {
          totalDataSize += key.getDataSize();
        }
      }

      if (totalDataSize < 0) {
        totalDataSize = 0;
      }

      OmBucketInfo.Builder bucketInfoBuilder = bucketInfo.toBuilder();

      omLockDetails = metadataManager.getLock()
              .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName);
      acquiredLock = omLockDetails.isLockAcquired();

      bucketInfoBuilder.setUsedBytes(totalDataSize);
      bucketInfo = bucketInfoBuilder.build();
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      metadataManager.getBucketTable().put(bucketKey, bucketInfo);
    } catch (OMException e) {
      LOG.error("Bucket not found: {}/{}", volumeName, bucketName, e);
      buildFailedOMResponse(omResponse, Status.BUCKET_NOT_FOUND, e);
      return new OMRefreshBucketUsedBytesResponse(omResponse.build());
    } catch (Exception e) {
      LOG.error("Unexpected error while updating usedBytes: ", e);
      buildFailedOMResponse(omResponse, Status.INTERNAL_ERROR, e);
      return new OMRefreshBucketUsedBytesResponse(omResponse.build());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(metadataManager.getLock()
                .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
    }
    LOG.info("UsedBytes was updated for bucket {}/{}: {}", volumeName, bucketName, totalDataSize);
    buildSuccessfulOMResponse(totalDataSize, omResponse);
    return new OMRefreshBucketUsedBytesResponse(omResponse.build());
  }

  private void buildSuccessfulOMResponse(long totalDataSize, OMResponse.Builder omResponse) {
    RefreshBucketUsedBytesResponse refreshBucketUsedBytesResponse = RefreshBucketUsedBytesResponse.newBuilder()
            .setUsedBytes(totalDataSize)
            .build();
    omResponse.setRefreshBucketUsedBytesResponse(refreshBucketUsedBytesResponse);
  }

  private void buildFailedOMResponse(OMResponse.Builder omResponse, Status status, Exception e) {
    omResponse.setSuccess(false)
            .setStatus(status)
            .setMessage(e.getMessage())
            .build();
  }
}
