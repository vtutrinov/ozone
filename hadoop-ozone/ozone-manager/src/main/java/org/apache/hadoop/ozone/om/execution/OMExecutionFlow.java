/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.execution;

import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.OMInSafeModeException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.RateLimiterManager;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.util.OzoneMultiRaftUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * entry for execution flow for write request.
 */
public class OMExecutionFlow {

  private static final Logger LOG = LoggerFactory.getLogger(OMExecutionFlow.class);

  private final OzoneManager ozoneManager;
  private final OMPerformanceMetrics perfMetrics;

  public OMExecutionFlow(OzoneManager om) {
    this.ozoneManager = om;
    this.perfMetrics = ozoneManager.getPerfMetrics();
  }

  /**
   * External request handling.
   * 
   * @param omRequest the request
   * @return OMResponse the response of execution
   * @throws ServiceException the exception on execution
   */
  public OMResponse submit(OMRequest omRequest, boolean isWrite) throws ServiceException {
    // TODO: currently have only execution after ratis submission, but with new flow can have switch later
    return submitExecutionToRatis(omRequest, isWrite);
  }

  private static String getMultiRaftBucketName(OMRequest request) {
    final String bucketName = OzoneMultiRaftUtils.getBucketName(request);
    return bucketName == null || bucketName.isEmpty() ? null : bucketName;
  }

  /**
   * SDP (multi-raft): the raft group a write request is submitted to: the group given by the client (retry after
   * OMNotLeaderException), the raft group of the bucket (assigned on first use, SDPOZN-1979), or the main OM group
   * for requests without a bucket. The group is derived from the raw request, so that the leader status can be
   * checked before the request is created: creating it reads the bucket from the local DB, which may lag on a follower.
   */
  public RaftGroupId resolveMultiRaftGroup(OMRequest request) {
    if (request.hasRaftGroupId()) {
      final HddsProtos.UUID uuid = request.getRaftGroupId();
      return RaftGroupId.valueOf(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
    }
    final String bucketName = getMultiRaftBucketName(request);
    return bucketName != null
        ? ozoneManager.raftGroupName(OzoneMultiRaftUtils.getVolumeName(request), bucketName)
        : ozoneManager.getOmRatisServer().getRaftGroupId();
  }

  /**
   * SDP (multi-raft): write request handling when bucket raft groups are enabled.
   * Bucket write requests go to the raft group of the bucket, others to the OM raft group;
   * leader status and retry cache are those of the target raft group.
   *
   * @param checkLeader whether to check that this OM is the leader of the target raft group
   */
  public OMResponse submitMultiRaftWrite(OMRequest request, boolean checkLeader) throws ServiceException {
    final OzoneManagerRatisServer ratisServer = ozoneManager.getOmRatisServer();
    // The target raft group is derived from the raw request, so that the leader status is checked before the
    // request is created: creating it reads the bucket from the local DB, which may lag on a follower.
    final String bucketName = getMultiRaftBucketName(request);
    final RaftGroupId raftGroupId;
    try {
      raftGroupId = resolveMultiRaftGroup(request);
    } catch (IllegalStateException e) {
      // the raft group assignment of the bucket failed (SDPOZN-1979)
      throw new ServiceException(e.getMessage(), e);
    }
    LOG.trace("Continue internal processing request {}, bucket {}, group {}",
        request.getCmdType(), bucketName, raftGroupId);
    if (checkLeader) {
      if (bucketName != null || request.hasRaftGroupId()) {
        OzoneManagerRatisUtils.checkLeaderStatus(raftGroupId, ozoneManager);
      } else {
        OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);
      }
    }
    final OMResponse cached = ratisServer.checkRetryCache(raftGroupId);
    if (cached != null) {
      return cached;
    }
    final OMResponse rateLimited = checkWriteRateLimit(request);
    if (rateLimited != null) {
      return rateLimited;
    }

    OMClientRequest omClientRequest = null;
    final OMRequest requestToSubmit;
    try {
      omClientRequest = OzoneManagerRatisUtils.createClientRequest(request, ozoneManager);
      assert (omClientRequest != null);
      final OMClientRequest finalOmClientRequest = omClientRequest;
      requestToSubmit = captureLatencyNs(perfMetrics.getPreExecuteLatencyNs(),
          () -> finalOmClientRequest.preExecute(ozoneManager));
    } catch (IOException ex) {
      if (omClientRequest != null) {
        OMAuditLogger.log(omClientRequest.getAuditBuilder());
        omClientRequest.handleRequestFailure(ozoneManager);
      }
      return OzoneManagerRatisUtils.createErrorResponse(request, ex);
    }

    if (bucketName != null) {
      try {
        // bucket raft groups are not usable while the OM is in (multi-raft) safe mode
        ozoneManager.getSafeModeManager().checkSafeMode();
      } catch (OMInSafeModeException ex) {
        LOG.error("OM is in safe mode, cannot process request: {}", request.getCmdType(), ex);
        omClientRequest.handleRequestFailure(ozoneManager);
        throw new ServiceException(ex);
      }
    }
    final OMResponse response = bucketName != null
        ? ratisServer.submitBucketWriteRequest(requestToSubmit, raftGroupId)
        : ratisServer.submitRequest(requestToSubmit, true);
    if (!response.getSuccess()) {
      omClientRequest.handleRequestFailure(ozoneManager);
    }
    return response;
  }

  private OMResponse submitExecutionToRatis(OMRequest request, boolean isWrite) throws ServiceException {
    // 1. create client request and preExecute
    OMClientRequest omClientRequest = null;
    final OMRequest requestToSubmit;
    if (isWrite) {
      final OMResponse rateLimited = checkWriteRateLimit(request);
      if (rateLimited != null) {
        return rateLimited;
      }
      try {
        omClientRequest = OzoneManagerRatisUtils.createClientRequest(request, ozoneManager);
        assert (omClientRequest != null);
        final OMClientRequest finalOmClientRequest = omClientRequest;
        requestToSubmit = captureLatencyNs(perfMetrics.getPreExecuteLatencyNs(),
            () -> finalOmClientRequest.preExecute(ozoneManager));
      } catch (IOException ex) {
        if (omClientRequest != null) {
          OMAuditLogger.log(omClientRequest.getAuditBuilder());
          omClientRequest.handleRequestFailure(ozoneManager);
        }
        return OzoneManagerRatisUtils.createErrorResponse(request, ex);
      }
    } else {
      requestToSubmit = request;
    }

    // 2. submit request to ratis
    OMResponse response = ozoneManager.getOmRatisServer().submitRequest(requestToSubmit, isWrite);
    if (!response.getSuccess() && omClientRequest != null) {
      omClientRequest.handleRequestFailure(ozoneManager);
    }
    return response;
  }

  /**
   * SDP (SDPOZN-1965): write rate limit of the bucket. Checked on the leader before the request is submitted to
   * Ratis: a rate limiter is local state, so checking it while applying the transaction would let the replicas
   * decide differently.
   * @return the error response if the request is rate limited, otherwise null
   */
  private OMResponse checkWriteRateLimit(OMRequest request) {
    final RateLimiterManager rateLimiterManager = ozoneManager.getRateLimiterManager();
    if (rateLimiterManager == null || rateLimiterManager.tryAcquire(request, true)) {
      return null;
    }
    return OmResponseUtil.getOMResponseBuilder(request)
        .setSuccess(false)
        .setStatus(OzoneManagerProtocolProtos.Status.RATE_LIMIT_EXCEEDED)
        .setMessage("Rate limit exceeded")
        .build();
  }
}
