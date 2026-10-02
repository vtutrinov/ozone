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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.validation.RequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint.LocalLeaseContext;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.S3SecurityUtil;
import org.apache.hadoop.ozone.util.OzoneMultiRaftUtils;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.ServerRpcProto;
import org.apache.ratis.protocol.RaftGroupId;
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

  private RaftGroupId resolveMultiRaftGroup(OMRequest request) throws ServiceException {
    try {
      return ozoneManager.getOmExecutionFlow().resolveMultiRaftGroup(request);
    } catch (IllegalStateException e) {
      throw new ServiceException(e.getMessage(), e);
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
          // SDP (multi-raft, SDPOZN-1979): with multi-raft a bucket write, or a read of the keys of a bucket, is
          // checked against the leader of the raft group of the bucket instead of the main OM raft group.
          final RaftGroupId leaderGroupId = !ozoneManager.isMultiRaftEnabled() ? null
              : OmUtils.isReadOnly(request) ? resolveReadRaftGroup(request) : resolveMultiRaftGroup(request);
          S3SecurityUtil.validateS3Credential(request, ozoneManager, leaderGroupId);
        } catch (IOException ex) {
          return createErrorResponse(request, ex);
        }
      }

      if (OmUtils.isReadOnly(request)) {
        return submitReadRequestToOM(request);
      }

      // SDP (multi-raft): bucket write requests go to the raft group of the bucket;
      // the leader status and retry cache of the target raft group are checked in OMExecutionFlow.
      if (ozoneManager.isMultiRaftEnabled()) {
        this.lastRequestToSubmit = request;
        return ozoneManager.getOmExecutionFlow().submitMultiRaftWrite(request, !s3Auth);
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
      return ozoneManager.getOmExecutionFlow().submit(request, true);
    } finally {
      OzoneManager.setS3Auth(null);
    }
  }

  @VisibleForTesting
  public OMRequest getLastRequestToSubmit() {
    return lastRequestToSubmit;
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    if (request.getCmdType().equals(PrepareStatus)) {
      // PrepareStatus is an OM request that only target a single OM node.
      // Therefore, all PrepareStatus requests should be served immediately without failover regardless
      // of the OM node leadership or the read consistency. See PrepareSubCommand.
      // The implementation is not ideal, but exists for compatibility reason.
      return handler.handleReadRequest(request);
    }

    // SDP (multi-raft): the leadership, lease or ReadIndex of the raft group the read is served from
    final RaftGroupId groupId = resolveReadRaftGroup(request);
    if (!OmUtils.specifiedReadConsistency(request)) {
      return submitReadRequestToOmWithoutHint(request, groupId);
    } else {
      // If read consistency hint is specified, we should try to respect it although
      // there is no guarantee since it depends on the OM node configuration (e.g.
      // whether OM Raft server enables linearizable read).
      ReadConsistencyHint readConsistencyHint = request.getReadConsistencyHint();
      ReadConsistencyProto readConsistency = readConsistencyHint.getReadConsistency();
      switch (readConsistency) {
      case LOCAL_LEASE:
        return submitReadRequestToOmLocalLease(request,
            readConsistencyHint.hasLocalLeaseContext() ? readConsistencyHint.getLocalLeaseContext() : null, groupId);
      case LINEARIZABLE_LEADER_ONLY:
        return submitReadRequestToOmLinearizableLeaderOnly(request, groupId);
      case LINEARIZABLE_ALLOW_FOLLOWER:
        return submitReadRequestToOmLinearizableAllowFollower(request, groupId);
      case DEFAULT:
      default:
        return submitReadRequestToOmDefault(request, groupId);
      }
    }
  }

  /**
   * SDP (multi-raft): the raft group whose state a read request is served from. The keys, files and multipart uploads
   * of a bucket assigned to a bucket raft group are written by that group, so their reads are checked against it
   * (its leader, lease or ReadIndex); other reads, and those of buckets without a group, against the OM raft group.
   */
  private RaftGroupId resolveReadRaftGroup(OMRequest request) {
    final RaftGroupId mainGroupId = omRatisServer.getRaftGroupId();
    if (!ozoneManager.isMultiRaftEnabled()) {
      return mainGroupId;
    }
    final String[] bucket = OzoneMultiRaftUtils.getReadRequestBucket(request);
    if (bucket == null) {
      return mainGroupId;
    }
    final UUID raftGroup;
    if (request.hasRaftGroupId()) {
      // retry after OMNotLeaderException of the raft group
      raftGroup = HddsUtils.fromProtobuf(request.getRaftGroupId());
    } else {
      raftGroup = ozoneManager.getOmRaftGroupManager().getBucketRaftGroups()
          .get(ozoneManager.getMetadataManager().getBucketKey(bucket[0], bucket[1]));
    }
    if (raftGroup == null || !ozoneManager.getOmRaftGroups().containsKey(RaftGroupId.valueOf(raftGroup))) {
      return mainGroupId;
    }
    return RaftGroupId.valueOf(raftGroup);
  }

  private boolean isMainGroup(RaftGroupId groupId) {
    return groupId.equals(omRatisServer.getRaftGroupId());
  }

  /**
   * Linearizable read through Ratis (ReadIndex), or a leader read if the Raft server read option is not LINEARIZABLE.
   * SDP (multi-raft): a read of a bucket raft group needs the state of the OM raft group (volumes, buckets, ACLs) to be
   * current as well, and a read of the OM raft group may find the bucket assigned to a bucket raft group once this OM
   * has caught up with the OM raft group.
   */
  private OMResponse submitLinearizableRead(OMRequest request, RaftGroupId groupId) throws ServiceException {
    if (isMainGroup(groupId)) {
      final OMResponse response = ozoneManager.getOmExecutionFlow().submit(request, false);
      final RaftGroupId assignedGroupId = resolveReadRaftGroup(request);
      return isMainGroup(assignedGroupId) ? response : omRatisServer.submitReadRequest(request, assignedGroupId);
    }
    if (omRatisServer.checkLeaderStatus(omRatisServer.getRaftGroupId()) != LEADER_AND_READY) {
      omRatisServer.submitReadRequest(newReadBarrier(), omRatisServer.getRaftGroupId());
    }
    return omRatisServer.submitReadRequest(request, groupId);
  }

  /** A read that does nothing: through Ratis, it waits until this OM has applied the ReadIndex of the raft group. */
  private OMRequest newReadBarrier() {
    return OMRequest.newBuilder()
        .setCmdType(Type.EchoRPC)
        .setClientId(omRatisServer.getCurrentClientId().toString())
        .setEchoRPCRequest(EchoRPCRequest.newBuilder().setReadOnly(true).setPayloadSizeResp(0))
        .build();
  }

  /** Follower local lease: the raft group, and the OM raft group for a bucket raft group, lag little. */
  private boolean allowLocalLease(RaftGroupId groupId, long leaseLogLimit, long leaseTimeMsLimit) {
    if (!allowFollowerReadLocalLease(omRatisServer.getServerDivision(groupId), leaseLogLimit, leaseTimeMsLimit)) {
      return false;
    }
    final RaftGroupId mainGroupId = omRatisServer.getRaftGroupId();
    return isMainGroup(groupId) || omRatisServer.checkLeaderStatus(mainGroupId) == LEADER_AND_READY
        || allowFollowerReadLocalLease(omRatisServer.getServerDivision(mainGroupId), leaseLogLimit, leaseTimeMsLimit);
  }

  private OMResponse submitReadRequestToOmWithoutHint(OMRequest request, RaftGroupId groupId)
      throws ServiceException {
    // Read from leader or followers using linearizable read
    if (ozoneManager.getConfig().isFollowerReadLocalLeaseEnabled() &&
        allowLocalLease(groupId,
            ozoneManager.getConfig().getFollowerReadLocalLeaseLogLimit(),
            ozoneManager.getConfig().getFollowerReadLocalLeaseTimeMs())) {
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseSuccess();
      return handler.handleReadRequest(request);
    }
    // Get current OM's role
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus(groupId);
    // === 1. Follower linearizable read ===
    if (raftServerStatus == NOT_LEADER && omRatisServer.isLinearizableRead()) {
      ozoneManager.getMetrics().incNumLinearizableRead();
      return submitLinearizableRead(request, groupId);
    }
    // === 2. Leader local read (skip ReadIndex if allowed) ===
    if (raftServerStatus == LEADER_AND_READY) {
      if (ozoneManager.getConfig().isAllowLeaderSkipLinearizableRead()) {
        ozoneManager.getMetrics().incNumLeaderSkipLinearizableRead();
        // leader directly serves local committed data
        return handler.handleReadRequest(request);
      }
      // otherwise use linearizable path when enabled
      if (omRatisServer.isLinearizableRead()) {
        ozoneManager.getMetrics().incNumLinearizableRead();
        return submitLinearizableRead(request, groupId);
      }

      // fallback to local read
      return handler.handleReadRequest(request);
    } else {
      throw createLeaderErrorException(raftServerStatus, groupId);
    }
  }

  private OMResponse submitReadRequestToOmLocalLease(OMRequest request, LocalLeaseContext localLeaseContext,
      RaftGroupId groupId) throws ServiceException {
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus(groupId);
    switch (raftServerStatus) {
    case NOT_LEADER:
    case LEADER_AND_NOT_READY:
      if (!ozoneManager.getConfig().isFollowerReadLocalLeaseEnabled()) {
        throw createLeaderErrorException(raftServerStatus, groupId);
      }
      long localLeaseLogLimit = localLeaseContext != null && localLeaseContext.hasLogLimit() ?
          localLeaseContext.getLogLimit() : ozoneManager.getConfig().getFollowerReadLocalLeaseLogLimit();
      long localLeaseLeaseTimeMs = localLeaseContext != null && localLeaseContext.hasLeaseTimeMs() ?
          localLeaseContext.getLeaseTimeMs() : ozoneManager.getConfig().getFollowerReadLocalLeaseTimeMs();
      if (allowLocalLease(groupId, localLeaseLogLimit, localLeaseLeaseTimeMs)) {
        ozoneManager.getMetrics().incNumFollowerReadLocalLeaseSuccess();
        return handler.handleReadRequest(request);
      }
      // The LocalLease lag is too high, trigger failover
      throw createLeaderErrorException(raftServerStatus, groupId);
    case LEADER_AND_READY:
      // Although local lease does not apply for leader (since leader is always up-to-date)
      // We still add the local lease metrics for compatibility reasons
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseSuccess();
      return handler.handleReadRequest(request);
    default:
      throw createUnknownRaftServerStatusException(raftServerStatus);
    }
  }

  private OMResponse submitReadRequestToOmLinearizableLeaderOnly(OMRequest request, RaftGroupId groupId)
      throws ServiceException {
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus(groupId);
    switch (raftServerStatus) {
    case NOT_LEADER:
    case LEADER_AND_NOT_READY:
      throw createLeaderErrorException(raftServerStatus, groupId);
    case LEADER_AND_READY:
      if (omRatisServer.isLinearizableRead()) {
        ozoneManager.getMetrics().incNumLinearizableRead();
        return submitLinearizableRead(request, groupId);
      } else {
        // If linearizable read is not enabled, fallback to leader read
        return handler.handleReadRequest(request);
      }
    default:
      throw createUnknownRaftServerStatusException(raftServerStatus);
    }
  }

  private OMResponse submitReadRequestToOmLinearizableAllowFollower(OMRequest request, RaftGroupId groupId)
      throws ServiceException {
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus(groupId);
    switch (raftServerStatus) {
    case LEADER_AND_NOT_READY:
    case NOT_LEADER:
      if (omRatisServer.isLinearizableRead()) {
        ozoneManager.getMetrics().incNumLinearizableRead();
        return submitLinearizableRead(request, groupId);
      } else {
        throw createLeaderErrorException(raftServerStatus, groupId);
      }
    case LEADER_AND_READY:
      if (ozoneManager.getConfig().isAllowLeaderSkipLinearizableRead()) {
        ozoneManager.getMetrics().incNumLeaderSkipLinearizableRead();
        // leader directly serves local committed data
        return handler.handleReadRequest(request);
      }

      // If the Raft server read option is not LINEARIZABLE, this will
      // use leader read
      if (omRatisServer.isLinearizableRead()) {
        ozoneManager.getMetrics().incNumLinearizableRead();
      }
      return submitLinearizableRead(request, groupId);
    default:
      throw createUnknownRaftServerStatusException(raftServerStatus);
    }
  }

  private OMResponse submitReadRequestToOmDefault(OMRequest request, RaftGroupId groupId) throws ServiceException {
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus(groupId);
    switch (raftServerStatus) {
    case LEADER_AND_READY:
      return handler.handleReadRequest(request);
    case LEADER_AND_NOT_READY:
    case NOT_LEADER:
      throw createLeaderErrorException(raftServerStatus, groupId);
    default:
      throw createUnknownRaftServerStatusException(raftServerStatus);
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

    if (leaseTimeMsLimit != -1 && leaderInfo.getLastRpcElapsedTimeMs() > leaseTimeMsLimit) {
      LOG.debug("FollowerRead Local Lease not allowed: Local lease Time expired. ");
      ozoneManager.getMetrics().incNumFollowerReadLocalLeaseFailTime();
      return false; // lease time expired
    }

    if (leaseLogLimit == -1) {
      // Allow infinite log lag, which allows unbounded stale reads
      // There is no need to check the leader lag
      return true;
    }

    final RaftPeerId leaderId = divisionInfo.getLeaderId();
    Long leaderCommit = null;
    if (leaderId != null) {
      for (CommitInfoProto i : ratisDivision.getCommitInfos()) {
        if (i.getServer().getId().equals(leaderId.toByteString())) {
          leaderCommit = i.getCommitIndex();
          break;
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
      RaftServerStatus raftServerStatus) {
    if (raftServerStatus == NOT_LEADER) {
      OMNotLeaderException notLeaderException = omRatisServer.newOMNotLeaderException();
      LOG.warn(notLeaderException.getMessage());
      return new ServiceException(notLeaderException);
    } else {
      return createLeaderNotReadyException();
    }
  }

  /** SDP (multi-raft): OMNotLeaderException of the raft group the request is served from. */
  private ServiceException createLeaderErrorException(RaftServerStatus raftServerStatus, RaftGroupId groupId) {
    if (isMainGroup(groupId)) {
      return createLeaderErrorException(raftServerStatus);
    }
    if (raftServerStatus == NOT_LEADER) {
      OMNotLeaderException notLeaderException = omRatisServer.newOMNotLeaderException(groupId);
      LOG.debug(notLeaderException.getMessage());
      return new ServiceException(notLeaderException);
    }
    return createLeaderNotReadyException();
  }

  private ServiceException createLeaderNotReadyException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  private ServiceException createUnknownRaftServerStatusException(RaftServerStatus raftServerStatus) {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();
    return new ServiceException(
        new OMException(raftPeerId.toString() + " has unknown raftServerStatus " + raftServerStatus,
            ResultCodes.INTERNAL_ERROR));
  }

  public static Logger getLog() {
    return LOG;
  }
}
