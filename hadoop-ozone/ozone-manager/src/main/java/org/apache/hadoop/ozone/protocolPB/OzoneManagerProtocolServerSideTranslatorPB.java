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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils.createErrorResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareStatus;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;
import static org.apache.hadoop.ozone.util.OzoneMultiRaftUtils.isMultiRaftEnabled;
import static org.apache.hadoop.ozone.util.OzoneRaftGroupIdGenerator.generateLimitedRaftGroupId;
import static org.apache.hadoop.ozone.util.OzoneRaftGroupIdGenerator.generateRaftGroupId;
import static org.apache.hadoop.util.MetricUtil.captureLatencyNs;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.hdds.protocol.OMInSafeModeException;
import org.apache.ratis.protocol.RaftGroupId;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.validation.RequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.S3SecurityUtil;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.ServerRpcProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer.Division;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the server-side translator that forwards requests received
 * from {@link OzoneManagerProtocolPB} to {@link OzoneManager}.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private static final String OM_REQUESTS_PACKAGE = "org.apache.hadoop.ozone";
  // same as hadoop ipc config defaults
  public static final String MAXIMUM_RESPONSE_LENGTH = "ipc.maximum.response.length";
  public static final int MAXIMUM_RESPONSE_LENGTH_DEFAULT = 134217728;

  private final int maxResponseLength;
  private final OzoneManagerRatisServer omRatisServer;
  private final RequestHandler handler;
  private final OzoneManager ozoneManager;
  private final OzoneProtocolMessageDispatcher<OMRequest, OMResponse,
      OzoneManagerProtocolProtos.Type> dispatcher;
  private final RequestValidations requestValidations;
  private final OMPerformanceMetrics perfMetrics;

  private OMRequest lastRequestToSubmit;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl,
      OzoneManagerRatisServer ratisServer,
      ProtocolMessageMetrics<OzoneManagerProtocolProtos.Type> metrics) {
    this.ozoneManager = impl;
    this.perfMetrics = impl.getPerfMetrics();

    this.handler = new OzoneManagerRequestHandler(impl);
    this.omRatisServer = ratisServer;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OzoneProtocol",
        metrics, LOG, OMPBHelper::processForDebug, OMPBHelper::processForDebug);

    // TODO: make this injectable for testing...
    this.requestValidations = new RequestValidations()
        .fromPackage(OM_REQUESTS_PACKAGE)
        .withinContext(ValidationContext.of(ozoneManager.getVersionManager(), ozoneManager.getMetadataManager()))
        .load();
    maxResponseLength = ozoneManager.getConfiguration()
        .getInt(MAXIMUM_RESPONSE_LENGTH, MAXIMUM_RESPONSE_LENGTH_DEFAULT);
  }

  /**
   * Submit mutating requests to Ratis server in OM, and process read requests.
   */
  @Override
  public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {
    OMRequest validatedRequest;
    try {
      validatedRequest = captureLatencyNs(
          perfMetrics.getValidateRequestLatencyNs(),
          () -> requestValidations.validateRequest(request));
    } catch (Exception e) {
      if (e instanceof OMException) {
        return createErrorResponse(request, (OMException) e);
      }
      throw new ServiceException(e);
    }

    OMResponse response = dispatcher.processRequest(validatedRequest,
        this::processRequest, request.getCmdType(), request.getTraceID());

    logLargeResponseIfNeeded(response);

    return captureLatencyNs(perfMetrics.getValidateResponseLatencyNs(),
        () -> requestValidations.validateResponse(request, response));
  }

  @VisibleForTesting
  public OMResponse processRequest(OMRequest request) throws ServiceException {
    OMResponse response = internalProcessRequest(request);
    if (response.hasOmLockDetails()) {
      OzoneManagerProtocolProtos.OMLockDetailsProto omLockDetailsProto =
          response.getOmLockDetails();
      Server.Call call = Server.getCurCall().get();
      if (call != null) {
        call.getProcessingDetails().add(Timing.LOCKWAIT,
            omLockDetailsProto.getWaitLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(Timing.LOCKSHARED,
            omLockDetailsProto.getReadLockNanos(), TimeUnit.NANOSECONDS);
        call.getProcessingDetails().add(Timing.LOCKEXCLUSIVE,
            omLockDetailsProto.getWriteLockNanos(), TimeUnit.NANOSECONDS);
      }
    }
    return response;
  }

  /**
   * Logs a warning if the OMResponse size exceeds half of the IPC maximum
   * response size threshold.
   *
   * @param response The OMResponse to check
   */
  @VisibleForTesting
  public void logLargeResponseIfNeeded(OMResponse response) {
    try {
      long warnThreshold = maxResponseLength / 2;
      long respSize = response.getSerializedSize();
      if (respSize > warnThreshold) {
        LOG.warn("Large OMResponse detected: cmd={} size={}B threshold={}B ",
            response.getCmdType(), respSize, warnThreshold);
      }
    } catch (Exception e) {
      LOG.info("Failed to log response size", e);
    }
  }

  private OMResponse internalProcessRequest(OMRequest request) throws ServiceException {
    boolean s3Auth = false;

    try {
      if (request.hasS3Authentication()) {
        OzoneManager.setS3Auth(request.getS3Authentication());
        try {
          s3Auth = true;
          // If request has S3Authentication, validate S3 credentials.
          // If current OM is leader and then proceed with the request.
          S3SecurityUtil.validateS3Credential(request, ozoneManager);
        } catch (IOException ex) {
          return createErrorResponse(request, ex);
        }
      }

      if (OmUtils.isReadOnly(request)) {
        return submitReadRequestToOM(request);
      }

      // To validate credentials we have already verified leader status.
      // This will skip of checking leader status again if request has S3Auth.
      if (!s3Auth) {
        OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);
      }

      // check retry cache
      final OMResponse cached = omRatisServer.checkRetryCache();
      if (cached != null) {
        return cached;
      }

      this.lastRequestToSubmit = request;
//      return ozoneManager.getOmExecutionFlow().submit(request, true);

      OMRequest requestToSubmit;
      try {
        omClientRequest = createClientRequest(request, ozoneManager);
        // check retry cache
        String volumeName = omClientRequest.getWriteReqVolumeName();
        String bucketName = omClientRequest.getWriteReqBucketName();

        LOG.trace("Continue internal processing request {}, bucket {}", request.getCmdType(), bucketName);
        // To validate credentials we have already verified leader status.
        // This will skip of checking leader status again if request has S3Auth.
        if (!s3Auth) {
          if (request.hasRaftGroupId()) {
            OzoneManagerRatisUtils.checkLeaderStatus(omClientRequest.getWriteRaftGroup(), ozoneManager);
          } else {
            OzoneManagerRatisUtils.checkLeaderStatus(volumeName, bucketName, ozoneManager);
          }
        }
        // TODO: Note: Due to HDDS-6055, createClientRequest() could now
        //  return null, which triggered the findbugs warning.
        //  Added the assertion.
        assert (omClientRequest != null);
        requestToSubmit = preExecute(omClientRequest);
      } catch (IOException ex) {
        if (omClientRequest != null) {
          omClientRequest.handleRequestFailure(ozoneManager);
        }
        return createErrorResponse(request, ex);
      }

      final OMResponse response;
      if (omClientRequest.getWriteReqBucketName() != null && ozoneManager.isMultiRaftEnabled()) {
        try {
          ozoneManager.getSafeModeManager().checkSafeMode();
        } catch (OMInSafeModeException ex) {
          LOG.error("OM is in safe mode, cannot process request: {}", request.getCmdType(), ex);
          throw new ServiceException(ex);
        }
        response = omRatisServer.submitBucketWriteRequest(
                requestToSubmit,
                omClientRequest.getWriteReqVolumeName(),
                omClientRequest.getWriteReqBucketName()
        );
      } else {
        response = omRatisServer.submitRequest(requestToSubmit);
      }

      if (!response.getSuccess()) {
        omClientRequest.handleRequestFailure(ozoneManager);
      }
      return response;
    } finally {
      OzoneManager.setS3Auth(null);
    }
  }

  @VisibleForTesting
  public OMRequest getLastRequestToSubmit() {
    return lastRequestToSubmit;
  }

  private OMRequest preExecute(OMClientRequest finalOmClientRequest)
      throws IOException {
    return captureLatencyNs(perfMetrics.getPreExecuteLatencyNs(),
        () -> finalOmClientRequest.preExecute(ozoneManager));
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Read from leader or followers using linearizable read
    if (ozoneManager.getConfig().isFollowerReadLocalLeaseEnabled() &&
        allowFollowerReadLocalLease(omRatisServer.getServerDivision(),
            ozoneManager.getConfig().getFollowerReadLocalLeaseLagLimit(),
            ozoneManager.getConfig().getFollowerReadLocalLeaseTimeMs())) {
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseSuccess();
      return handler.handleReadRequest(request);
    } 
    // Get current OM's role
    RaftServerStatus raftServerStatus = omRatisServer.getLeaderStatus();
    // === 1. Follower linearizable read ===
    if (raftServerStatus == NOT_LEADER && omRatisServer.isLinearizableRead()) {
      ozoneManager.getMetrics().incNumLinearizableRead();
      return ozoneManager.getOmExecutionFlow().submit(request, false);
    }
    // === 2. Leader local read (skip ReadIndex if allowed) ===
    if (raftServerStatus == LEADER_AND_READY || request.getCmdType().equals(PrepareStatus)) {
      if (ozoneManager.getConfig().isAllowLeaderSkipLinearizableRead()) {
        ozoneManager.getMetrics().incNumLeaderSkipLinearizableRead();
        // leader directly serves local committed data
        return handler.handleReadRequest(request);
      }
      // otherwise use linearizable path when enabled
      if (omRatisServer.isLinearizableRead()) {
        ozoneManager.getMetrics().incNumLinearizableRead();
        return ozoneManager.getOmExecutionFlow().submit(request, false);
      }

      // fallback to local read
      return handler.handleReadRequest(request);
    } else {
      throw createLeaderErrorException(omRatisServer.getCurrentRaftGroupId(), raftServerStatus);
    }
  }

  boolean allowFollowerReadLocalLease(Division ratisDivision, long leaseLogLimit, long leaseTimeMsLimit) {
    final DivisionInfo divisionInfo = ratisDivision.getInfo();
    final FollowerInfoProto followerInfo = divisionInfo.getRoleInfoProto().getFollowerInfo();
    if (followerInfo == null) {
      LOG.debug("FollowerRead Local Lease not allowed: Not a follower. ");
      return false; // not follower
    }
    final ServerRpcProto leaderInfo = followerInfo.getLeaderInfo();
    if (leaderInfo == null) {
      LOG.debug("FollowerRead Local Lease not allowed: No Leader ");
      return false; // no leader
    }

    if (leaderInfo.getLastRpcElapsedTimeMs() > leaseTimeMsLimit) {
      LOG.debug("FollowerRead Local Lease not allowed: Local lease Time expired. ");
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseFailTime();
      return false; // lease time expired
    }

    final RaftPeerId leaderId = divisionInfo.getLeaderId();
    Long leaderCommit = null;
    if (leaderId != null) {
      for (CommitInfoProto i : ratisDivision.getCommitInfos()) {
        if (i.getServer().getId().equals(leaderId.toByteString())) {
          leaderCommit = i.getCommitIndex();
        }
      }
    }
    if (leaderCommit == null) {
      LOG.debug("FollowerRead Local Lease not allowed: Leader Commit not exists. ");
      return false;
    }

    boolean ret = divisionInfo.getLastAppliedIndex() + leaseLogLimit >= leaderCommit;
    if (!ret) {
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseFailLog();
      LOG.debug("FollowerRead Local Lease not allowed: Index Lag exceeds limit. ");
    }
    return ret;
  }

  private ServiceException createLeaderErrorException(
          RaftGroupId raftGroupId, RaftServerStatus raftServerStatus) {
    if (raftServerStatus == NOT_LEADER) {
      return new ServiceException(omRatisServer.newOMNotLeaderException(raftGroupId));
    } else {
      return createLeaderNotReadyException();
    }
  }

  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();
    RaftPeerId raftLeaderId = null;
    String raftLeaderAddress = null;
    RaftPeer leader = omRatisServer.getLeader();
    if (null != leader) {
      raftLeaderId = leader.getId();
      raftLeaderAddress = omRatisServer.getRaftLeaderAddress(leader);
    }

    OMNotLeaderException notLeaderException =
        raftLeaderId == null ? new OMNotLeaderException(raftPeerId, omRatisServer.getCurrentRaftGroupId()) :
            new OMNotLeaderException(raftPeerId, raftLeaderId,
                raftLeaderAddress, omRatisServer.getCurrentRaftGroupId());

    LOG.debug(notLeaderException.getMessage());

    return new ServiceException(notLeaderException);
  }

  private ServiceException createLeaderNotReadyException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  public static Logger getLog() {
    return LOG;
  }
}
