/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheControlRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheControlResponse;
import org.apache.hadoop.ozone.security.acl.AuthorizerCacheControl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OMAdminProtocolPB} to the OMAdminProtocolServer implementation.
 */
public class OMAdminProtocolServerSideImpl implements OMAdminProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAdminProtocolServerSideImpl.class);

  private final OzoneManager ozoneManager;

  public OMAdminProtocolServerSideImpl(OzoneManager om) {
    this.ozoneManager = om;
  }

  @Override
  public OMConfigurationResponse getOMConfiguration(RpcController controller,
      OMConfigurationRequest request) throws ServiceException {

    List<OMNodeDetails> oldOMNodesList = ozoneManager.getAllOMNodesInMemory();
    List<OMNodeDetails> newOMNodesList = ozoneManager.getAllOMNodesInNewConf();

    List<OMNodeInfo> omNodesInMemory = new ArrayList<>(oldOMNodesList.size());
    for (OMNodeDetails omNodeDetails : oldOMNodesList) {
      omNodesInMemory.add(omNodeDetails.getProtobuf());
    }

    List<OMNodeInfo> omNodesInNewConf =
        new ArrayList<>(newOMNodesList.size());
    for (OMNodeDetails omNodeDetails : newOMNodesList) {
      omNodesInNewConf.add(omNodeDetails.getProtobuf());
    }

    return OMConfigurationResponse.newBuilder()
        .setSuccess(true)
        .addAllNodesInMemory(omNodesInMemory)
        .addAllNodesInNewConf(omNodesInNewConf)
        .build();
  }

  @Override
  public DecommissionOMResponse decommission(RpcController controller,
      DecommissionOMRequest request) throws ServiceException {
    if (request == null) {
      return null;
    }
    if (!ozoneManager.isRatisEnabled()) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg("OM node cannot be decommissioned as Ratis is " +
              "not enabled.")
          .build();
    }

    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);

    OMNodeDetails decommNode = ozoneManager.getPeerNode(request.getNodeId());
    if (decommNode == null) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg("OM node not present in the OM peer list.")
          .build();
    }

    try {
      omRatisServer.removeOMFromRatisRing(decommNode);
    } catch (IOException ex) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg(ex.getMessage())
          .build();
    }

    return DecommissionOMResponse.newBuilder()
        .setSuccess(true)
        .build();
  }

  @Override
  public RangerCacheControlResponse rangerCacheControl(
      RpcController controller, RangerCacheControlRequest request)
      throws ServiceException {
    RangerCacheControlResponse.Builder response =
        RangerCacheControlResponse.newBuilder();

    UserGroupInformation ugi = Server.getRemoteUser();
    if (ugi == null || !ozoneManager.isAdmin(ugi)) {
      return response.setSuccess(false)
          .setErrorMsg("PERMISSION_DENIED: only Ozone admins may control " +
              "the Ranger policy cache. User: " +
              (ugi == null ? "unknown" : ugi.getUserName()))
          .build();
    }

    IAccessAuthorizer authorizer = ozoneManager.getAccessAuthorizer();
    if (!(authorizer instanceof AuthorizerCacheControl)) {
      return response.setSuccess(false)
          .setErrorMsg("NOT_SUPPORTED: configured authorizer " +
              (authorizer == null ? "null" : authorizer.getClass().getName()) +
              " does not support cache control")
          .build();
    }

    AuthorizerCacheControl cacheControl = (AuthorizerCacheControl) authorizer;
    String name = request.hasName() ? request.getName() : null;
    try {
      switch (request.getOp()) {
      case RANGER_CACHE_STATUS:
        response.setStatusJson(cacheControl.getCacheStatus());
        break;
      case RANGER_CACHE_INVALIDATE:
        LOG.warn("Ranger policy cache invalidation requested by admin {}",
            ugi.getUserName());
        cacheControl.invalidateCache();
        break;
      case RANGER_CACHE_INVALIDATE_POLICY:
        requireName(name, "INVALIDATE_POLICY");
        LOG.warn("Ranger cached policy '{}' invalidation requested by " +
            "admin {}", name, ugi.getUserName());
        response.setEntryFound(cacheControl.invalidateCachedPolicy(name));
        break;
      case RANGER_CACHE_INVALIDATE_ROLE:
        requireName(name, "INVALIDATE_ROLE");
        LOG.warn("Ranger cached role '{}' invalidation requested by " +
            "admin {}", name, ugi.getUserName());
        response.setEntryFound(cacheControl.invalidateCachedRole(name));
        break;
      case RANGER_CACHE_EXTEND:
        if (!request.hasTtlMillis() || request.getTtlMillis() <= 0) {
          throw new IOException("EXTEND requires a positive ttlMillis");
        }
        LOG.info("Ranger policy cache validity extension of {} ms " +
            "requested by admin {}", request.getTtlMillis(),
            ugi.getUserName());
        cacheControl.extendCacheValidity(request.getTtlMillis());
        break;
      default:
        throw new IOException("Unknown cache control op: " + request.getOp());
      }
      response.setSuccess(true);
    } catch (Throwable t) {
      LOG.error("Ranger cache control op {} failed", request.getOp(), t);
      response.setSuccess(false)
          .setErrorMsg(t.getMessage() == null
              ? t.getClass().getName() : t.getMessage());
    }
    return response.build();
  }

  private static void requireName(String name, String op) throws IOException {
    if (name == null || name.isEmpty()) {
      throw new IOException(op + " requires a policy/role name");
    }
  }
}
