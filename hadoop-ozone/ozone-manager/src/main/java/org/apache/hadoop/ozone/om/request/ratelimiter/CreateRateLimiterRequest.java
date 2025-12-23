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
import org.apache.hadoop.ozone.om.response.ratelimiter.CreateRateLimiterResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.toDBKey;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * OM client request that creates a new rate limiter
 * and persists its configuration in OM.
 */
public class CreateRateLimiterRequest extends OMClientRequest {

  public static final Logger LOG = LoggerFactory.getLogger(CreateRateLimiterRequest.class);

  public CreateRateLimiterRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long trxnLogIndex) {
    OMRequest omRequest = getOmRequest();
    OzoneManagerProtocolProtos.CreateRateLimiterRequest createRateLimiterResponse =
            getOmRequest().getCreateRateLimiterRequest();
    String volumeName = createRateLimiterResponse.getVolumeName();
    String bucketName = createRateLimiterResponse.getBucketName();
    int rps = createRateLimiterResponse.getRps();
    OzoneManagerProtocolProtos.RateLimiterType type = createRateLimiterResponse.getType();
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(omRequest);
    boolean bucketLockAcquired = false;
    RateLimiterInfo rateLimiterInfo = null;
    Exception exception = null;
    OzoneManagerProtocolProtos.Status status = OzoneManagerProtocolProtos.Status.OK;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    String errorMsg = null;

    try {
      metadataManager.getLock().acquireWriteLock(
              BUCKET_LOCK, volumeName, bucketName);
      bucketLockAcquired = true;
      Table<String, RateLimiterInfo> rateLimiterTable =
              metadataManager.getRateLimiterInfoTable();
      String key = toDBKey(metadataManager, volumeName, bucketName, type);
      RateLimiterInfo existing = rateLimiterTable.get(key);
      if (existing != null) {
        errorMsg = "Rate limiter already exists for volume=" + volumeName
                + ", bucket=" + bucketName + ", type=" + type.name();
        LOG.error(errorMsg);
        status = OzoneManagerProtocolProtos.Status.RATE_LIMITER_ALREADY_EXISTS;
        rateLimiterInfo = existing;
      } else {
        rateLimiterInfo = RateLimiterInfo.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setRps(rps)
                .setType(type)
                .build();

        rateLimiterTable.put(key, rateLimiterInfo);
        ozoneManager.getRateLimiterManager().createOrUpdate(rateLimiterInfo);
        LOG.info("Created rate limiter: key={}, rps={}, type={}",
                key, rps, type);
      }
    } catch (IOException e) {
      exception = e;
      status = OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
      errorMsg = "Failed to create rate limiter for volume=" + volumeName
              + ", bucket=" + bucketName + ": " + e.getMessage();
      LOG.error(errorMsg, e);
    } finally {
      if (bucketLockAcquired) {
        metadataManager.getLock().releaseWriteLock(
                BUCKET_LOCK, volumeName, bucketName);
      }
    }
    OzoneManagerProtocolProtos.CreateRateLimiterResponse.Builder createRespBuilder =
            OzoneManagerProtocolProtos.CreateRateLimiterResponse.newBuilder();

    createRespBuilder.setRateLimiter(rateLimiterInfo.toProtobuf());
    omResponse.setStatus(status)
            .setCreateRateLimiterResponse(createRespBuilder.build());

    if (status == OzoneManagerProtocolProtos.Status.OK) {
      Map<String, String> auditMap = new LinkedHashMap<>();
      auditMap.put("volume", volumeName);
      auditMap.put("bucket", bucketName);
      auditMap.put("rps", String.valueOf(rps));
      auditMap.put("type", type.name());

      auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_RATELIMITER,
              auditMap, exception, userInfo));
    }

    if (errorMsg != null) {
      omResponse.setMessage(errorMsg);
    }

    return new CreateRateLimiterResponse(omResponse.build());
  }
}
