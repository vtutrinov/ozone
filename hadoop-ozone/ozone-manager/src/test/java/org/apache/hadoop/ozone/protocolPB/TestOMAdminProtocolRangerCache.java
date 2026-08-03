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

import java.io.IOException;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheControlRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheControlResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheOpType;
import org.apache.hadoop.ozone.security.acl.AuthorizerCacheControl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the rangerCacheControl handler of OMAdminProtocolServerSideImpl.
 */
public class TestOMAdminProtocolRangerCache {

  private OzoneManager ozoneManager;
  private OMAdminProtocolServerSideImpl subject;
  private UserGroupInformation ugi;

  /**
   * Authorizer implementing both IAccessAuthorizer and cache control.
   */
  private abstract static class CacheControlledAuthorizer
      implements IAccessAuthorizer, AuthorizerCacheControl {
    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return true;
    }
  }

  @BeforeEach
  public void setup() {
    ozoneManager = mock(OzoneManager.class);
    subject = new OMAdminProtocolServerSideImpl(ozoneManager);
    ugi = UserGroupInformation.createRemoteUser("testadmin");
  }

  private RangerCacheControlResponse invoke(RangerCacheControlRequest request)
      throws Exception {
    try (MockedStatic<Server> server = mockStatic(Server.class)) {
      server.when(Server::getRemoteUser).thenReturn(ugi);
      return subject.rangerCacheControl(null, request);
    }
  }

  private static RangerCacheControlRequest request(RangerCacheOpType op) {
    return RangerCacheControlRequest.newBuilder().setOp(op).build();
  }

  @Test
  public void deniedForNonAdmin() throws Exception {
    when(ozoneManager.isAdmin(ugi)).thenReturn(false);

    RangerCacheControlResponse response =
        invoke(request(RangerCacheOpType.RANGER_CACHE_INVALIDATE));

    assertFalse(response.getSuccess());
    assertTrue(response.getErrorMsg().contains("PERMISSION_DENIED"));
  }

  @Test
  public void deniedWithoutRemoteUser() throws Exception {
    // outside an RPC call Server.getRemoteUser() is null
    RangerCacheControlResponse response = subject.rangerCacheControl(null,
        request(RangerCacheOpType.RANGER_CACHE_INVALIDATE));

    assertFalse(response.getSuccess());
    assertTrue(response.getErrorMsg().contains("PERMISSION_DENIED"));
  }

  @Test
  public void notSupportedForPlainAuthorizer() throws Exception {
    when(ozoneManager.isAdmin(ugi)).thenReturn(true);
    when(ozoneManager.getAccessAuthorizer())
        .thenReturn(OzoneAccessAuthorizer.get());

    RangerCacheControlResponse response =
        invoke(request(RangerCacheOpType.RANGER_CACHE_STATUS));

    assertFalse(response.getSuccess());
    assertTrue(response.getErrorMsg().contains("NOT_SUPPORTED"));
  }

  @Test
  public void opsDispatchToAuthorizer() throws Exception {
    CacheControlledAuthorizer authorizer =
        mock(CacheControlledAuthorizer.class);
    when(ozoneManager.isAdmin(ugi)).thenReturn(true);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);
    when(authorizer.getCacheStatus()).thenReturn("{\"state\":\"FRESH\"}");
    when(authorizer.invalidateCachedPolicy("p1")).thenReturn(true);
    when(authorizer.invalidateCachedRole("r1")).thenReturn(false);

    RangerCacheControlResponse status =
        invoke(request(RangerCacheOpType.RANGER_CACHE_STATUS));
    assertTrue(status.getSuccess());
    assertEquals("{\"state\":\"FRESH\"}", status.getStatusJson());

    RangerCacheControlResponse invalidate =
        invoke(request(RangerCacheOpType.RANGER_CACHE_INVALIDATE));
    assertTrue(invalidate.getSuccess());
    verify(authorizer).invalidateCache();

    RangerCacheControlResponse policy = invoke(
        RangerCacheControlRequest.newBuilder()
            .setOp(RangerCacheOpType.RANGER_CACHE_INVALIDATE_POLICY)
            .setName("p1").build());
    assertTrue(policy.getSuccess());
    assertTrue(policy.getEntryFound());

    RangerCacheControlResponse role = invoke(
        RangerCacheControlRequest.newBuilder()
            .setOp(RangerCacheOpType.RANGER_CACHE_INVALIDATE_ROLE)
            .setName("r1").build());
    assertTrue(role.getSuccess());
    assertFalse(role.getEntryFound());

    RangerCacheControlResponse extend = invoke(
        RangerCacheControlRequest.newBuilder()
            .setOp(RangerCacheOpType.RANGER_CACHE_EXTEND)
            .setTtlMillis(60000).build());
    assertTrue(extend.getSuccess());
    verify(authorizer).extendCacheValidity(60000L);
  }

  @Test
  public void failuresAreReportedNotThrown() throws Exception {
    CacheControlledAuthorizer authorizer =
        mock(CacheControlledAuthorizer.class);
    when(ozoneManager.isAdmin(ugi)).thenReturn(true);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);
    when(authorizer.getCacheStatus())
        .thenThrow(new IOException("plugin not initialized"));

    RangerCacheControlResponse response =
        invoke(request(RangerCacheOpType.RANGER_CACHE_STATUS));

    assertFalse(response.getSuccess());
    assertTrue(response.getErrorMsg().contains("plugin not initialized"));
  }

  @Test
  public void invalidArgumentsAreRejected() throws Exception {
    CacheControlledAuthorizer authorizer =
        mock(CacheControlledAuthorizer.class);
    when(ozoneManager.isAdmin(ugi)).thenReturn(true);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);

    // INVALIDATE_POLICY without a name
    RangerCacheControlResponse noName = invoke(
        request(RangerCacheOpType.RANGER_CACHE_INVALIDATE_POLICY));
    assertFalse(noName.getSuccess());
    assertTrue(noName.getErrorMsg().contains("requires a policy/role name"));

    // EXTEND without ttl
    RangerCacheControlResponse noTtl = invoke(
        request(RangerCacheOpType.RANGER_CACHE_EXTEND));
    assertFalse(noTtl.getSuccess());
    assertTrue(noTtl.getErrorMsg().contains("positive ttlMillis"));

    verify(authorizer, org.mockito.Mockito.never())
        .invalidateCachedPolicy(any());
    verify(authorizer, org.mockito.Mockito.never())
        .extendCacheValidity(org.mockito.ArgumentMatchers.anyLong());
  }
}
