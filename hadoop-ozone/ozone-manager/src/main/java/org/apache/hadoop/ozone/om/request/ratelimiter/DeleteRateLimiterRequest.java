/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.request.ratelimiter;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.RateLimiterInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.ratelimiter.DeleteRateLimiterResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.toDBKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INVALID_REQUEST;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.RATELIMITER_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;

/**
 * OM client request that removes an existing rate limiter
 * from OM metadata and the in-memory RateLimiterManager.
 */
public class DeleteRateLimiterRequest extends OMClientRequest {
  public static final Logger LOG = LoggerFactory.getLogger(DeleteRateLimiterRequest.class);

  public DeleteRateLimiterRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
          OzoneManager ozoneManager, long trxnLogIndex) {

    OMRequest omRequest = getOmRequest();
    OzoneManagerProtocolProtos.DeleteRateLimiterRequest request =
            omRequest.getDeleteRateLimiterRequest();

    String volumeName = request.getVolumeName();
    String bucketName = request.getBucketName();
    OzoneManagerProtocolProtos.RateLimiterType type = request.getType();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(omRequest);

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    boolean bucketLockAcquired = false;
    OzoneManagerProtocolProtos.Status status = OK;
    String errorMsg = null;
    Exception exception = null;

    try {
      metadataManager.getLock()
              .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName);
      bucketLockAcquired = true;

      if (volumeName.isEmpty()) {
        status = INVALID_REQUEST;
        errorMsg = "Volume name must not be empty.";
      } else if (bucketName.isEmpty()) {
        status = INVALID_REQUEST;
        errorMsg = "Bucket name must not be empty.";
      }

      if (status == OK) {
        Table<String, RateLimiterInfo> table =
                metadataManager.getRateLimiterInfoTable();

        String key = toDBKey(metadataManager, volumeName, bucketName, type);
        RateLimiterInfo existing = null;
        try {
          existing = table.get(key);
        } catch (IOException e) {
          LOG.error("Cannot find ratelimiter for: {}", key);
        }
        if (existing == null) {
          status = RATELIMITER_NOT_FOUND;
          errorMsg = "Rate limiter not found for key " + key;
        } else {
          table.delete(key);
          ozoneManager.getRateLimiterManager()
                  .delete(volumeName, bucketName, type);
          LOG.info("Deleted rate limiter: key={}, type={}", key, type);
        }
      }

    } catch (IOException e) {
      exception = e;
      status = INTERNAL_ERROR;
      errorMsg = "Failed to delete rate limiter for volume=" + volumeName
              + ", bucket=" + bucketName + ": " + e.getMessage();
      LOG.error(errorMsg, e);
    } finally {
      if (bucketLockAcquired) {
        metadataManager.getLock()
                .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName);
      }
    }

    OzoneManagerProtocolProtos.DeleteRateLimiterResponse.Builder deleteRespBuilder =
            OzoneManagerProtocolProtos.DeleteRateLimiterResponse.newBuilder();

    omResponse.setStatus(status)
            .setDeleteRateLimiterResponse(deleteRespBuilder.build());

    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("volume", volumeName);
    auditMap.put("bucket", bucketName);
    auditMap.put("type", type.name());

    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_RATELIMITER,
            auditMap, exception, userInfo));

    if (errorMsg != null) {
      omResponse.setMessage(errorMsg);
    }

    return new DeleteRateLimiterResponse(omResponse.build());
  }
}
