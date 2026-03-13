/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OmRaftGroupManager} lock/release/assign behavior
 * in trySetAndGetRaftGroupToHandleBucketWriteRequest.
 */
public class TestOmRaftGroupManager {

  private OmRaftGroupManager manager;
  private OzoneManager ozoneManager;
  private OMMetadataManager metadataManager;

  private static final String VOL = "vol";
  private static final String BUCKET = "bucket";
  private static final String BUCKET_PATH = "/vol/bucket";

  @BeforeEach
  void setUp() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    metadataManager = mock(OMMetadataManager.class);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 1);

    when(metadataManager.getBucketKey(VOL, BUCKET)).thenReturn(BUCKET_PATH);

    OmRaftGroupManager real = new OmRaftGroupManager(
        ozoneManager, conf, true, "omService1", metadataManager);
    manager = spy(real);

    // Populate one raft group so selectLessLoadedRaftGroup works
    manager.addGroupIdToRaftGroupCounter(UUID.randomUUID());

    // Stub methods that use complex gRPC/Ratis infrastructure
    doNothing().when(manager).awaitBucketRaftGroupsInitialization();
    doNothing().when(manager).releaseBucketRaftGroupAssignmentWriteLockByRaft();
  }

  @Test
  void testReleaseLockCalledOnAcquireLockError() throws Exception {
    doThrow(new IOException("acquire failed"))
        .when(manager).acquireBucketRaftGroupAssignmentWriteLockByRaft();

    assertThrows(RuntimeException.class, () ->
        manager.getRaftGroupToHandleBucketWriteRequest(VOL, BUCKET));

    verify(manager).releaseBucketRaftGroupAssignmentWriteLockByRaft();
    verify(manager, never())
        .assignRaftGroupToBucket(anyString(), any(UUID.class));
    assertFalse(manager.getBucketAssignmentLocks().containsKey(BUCKET_PATH));
  }

  @Test
  void testReleaseLockCalledOnAssignError() throws Exception {
    doReturn(true)
        .when(manager).acquireBucketRaftGroupAssignmentWriteLockByRaft();
    doThrow(new IOException("assign failed"))
        .when(manager).assignRaftGroupToBucket(anyString(), any(UUID.class));

    assertThrows(RuntimeException.class, () ->
        manager.getRaftGroupToHandleBucketWriteRequest(VOL, BUCKET));

    verify(manager).releaseBucketRaftGroupAssignmentWriteLockByRaft();
    assertFalse(manager.getBucketAssignmentLocks().containsKey(BUCKET_PATH));
  }

  @Test
  void testReleaseLockCalledWhenBucketAlreadyAssigned() throws Exception {
    UUID preAssignedGroup = UUID.randomUUID();

    // Simulate another thread assigning the bucket while we wait for lock
    doAnswer(invocation -> {
      manager.getBucketRaftGroups().put(BUCKET_PATH, preAssignedGroup);
      return true;
    }).when(manager).acquireBucketRaftGroupAssignmentWriteLockByRaft();

    RaftGroupId result =
        manager.getRaftGroupToHandleBucketWriteRequest(VOL, BUCKET);

    assertEquals(RaftGroupId.valueOf(preAssignedGroup), result);
    // Release called in finally block
    verify(manager, times(1))
        .releaseBucketRaftGroupAssignmentWriteLockByRaft();
    verify(manager, never())
        .assignRaftGroupToBucket(anyString(), any(UUID.class));
    assertFalse(manager.getBucketAssignmentLocks().containsKey(BUCKET_PATH));
  }

  @Test
  void testSuccessfulAssignReleasesLockAndCleansUp() throws Exception {
    doReturn(true)
        .when(manager).acquireBucketRaftGroupAssignmentWriteLockByRaft();
    doNothing()
        .when(manager).assignRaftGroupToBucket(anyString(), any(UUID.class));

    RaftGroupId result =
        manager.getRaftGroupToHandleBucketWriteRequest(VOL, BUCKET);

    assertNotNull(result);
    verify(manager).releaseBucketRaftGroupAssignmentWriteLockByRaft();
    verify(manager, times(1))
        .assignRaftGroupToBucket(eq(BUCKET_PATH), any(UUID.class));
    assertFalse(manager.getBucketAssignmentLocks().containsKey(BUCKET_PATH));
  }
}
