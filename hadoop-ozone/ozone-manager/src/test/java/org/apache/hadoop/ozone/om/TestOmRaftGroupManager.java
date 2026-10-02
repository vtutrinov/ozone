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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.group.OMCreateRaftGroupsRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    // the main group leader returns the effective raft group of the bucket
    doAnswer(invocation -> invocation.getArgument(1))
        .when(manager).assignRaftGroupToBucket(anyString(), any(UUID.class));

    RaftGroupId result =
        manager.getRaftGroupToHandleBucketWriteRequest(VOL, BUCKET);

    assertNotNull(result);
    verify(manager).releaseBucketRaftGroupAssignmentWriteLockByRaft();
    verify(manager, times(1))
        .assignRaftGroupToBucket(eq(BUCKET_PATH), any(UUID.class));
    assertFalse(manager.getBucketAssignmentLocks().containsKey(BUCKET_PATH));
  }

  @Test
  void testBucketRaftGroupIdsCarryUniqueSerials() {
    List<RaftGroupId> groups = OmRaftGroupManager.generateRaftGroups(5, 3);
    assertEquals(3, groups.size());
    for (int i = 0; i < groups.size(); i++) {
      assertEquals(6 + i, OmRaftGroupManager.getBucketRaftGroupSerial(groups.get(i).getUuid()));
    }
    assertEquals(new HashSet<>(groups).size(), groups.size());
    // the OM raft group id carries no serial
    assertEquals(-1, OmRaftGroupManager.getBucketRaftGroupSerial(OmRaftGroupManager.toUuid("om-service")));
    assertThrows(IllegalArgumentException.class, () -> OmRaftGroupManager.bucketRaftGroupId(0));
    assertThrows(IllegalArgumentException.class,
        () -> OmRaftGroupManager.bucketRaftGroupId(OmRaftGroupManager.MAX_BUCKET_RAFT_GROUP_SERIAL + 1));
  }

  @Test
  void testExecutionIndexesOfRaftGroupsDoNotOverlap() {
    long maxIndex = OmRaftGroupManager.MAX_BUCKET_RAFT_GROUP_INDEX;
    // the OM raft group (serial 0) uses its log indexes up to maxIndex
    assertTrue(OmRaftGroupManager.toExecutionIndex(1, 1) > maxIndex);
    assertTrue(OmRaftGroupManager.toExecutionIndex(2, 1) > OmRaftGroupManager.toExecutionIndex(1, maxIndex));
    assertThrows(IllegalStateException.class, () -> OmRaftGroupManager.toExecutionIndex(1, maxIndex + 1));
    // object IDs are derived from the execution index
    long highest = OmRaftGroupManager.toExecutionIndex(OmRaftGroupManager.MAX_BUCKET_RAFT_GROUP_SERIAL, maxIndex);
    assertTrue(highest <= OmUtils.MAX_TRXN_ID);
    assertNotEquals(OmUtils.getObjectIdFromTxId(2, OmRaftGroupManager.toExecutionIndex(1, 7)),
        OmUtils.getObjectIdFromTxId(2, OmRaftGroupManager.toExecutionIndex(2, 7)));
  }

  @Test
  void testNewRaftGroupsMustHaveNewSerials() throws Exception {
    List<UUID> next = OmRaftGroupManager.generateRaftGroups(4, 2).stream()
        .map(RaftGroupId::getUuid).collect(Collectors.toList());
    assertEquals(6, OMCreateRaftGroupsRequest.validateSerials(4, next));
    // a serial allocated before, e.g. a duplicated or stale request
    assertThrows(OMException.class, () -> OMCreateRaftGroupsRequest.validateSerials(5, next));
    // not ascending
    List<UUID> reversed = new ArrayList<>(next);
    Collections.reverse(reversed);
    assertThrows(OMException.class, () -> OMCreateRaftGroupsRequest.validateSerials(4, reversed));
    // ids without serial
    assertThrows(OMException.class,
        () -> OMCreateRaftGroupsRequest.validateSerials(0, Collections.singletonList(UUID.randomUUID())));
  }

  @Test
  void testLegacyBucketRaftGroupIds() {
    Set<UUID> legacy = OmRaftGroupManager.legacyBucketRaftGroupIds(1);
    assertEquals(200, legacy.size());
    assertTrue(legacy.contains(OmRaftGroupManager.toUuid("0")));
    assertTrue(legacy.contains(OmRaftGroupManager.toUuid("199")));
    assertFalse(legacy.contains(OmRaftGroupManager.toUuid("200")));
    // neither current bucket raft groups nor other raft groups (e.g. random ids) are taken for 1.4 ones
    assertFalse(legacy.contains(OmRaftGroupManager.bucketRaftGroupId(1).getUuid()));
    assertFalse(legacy.contains(UUID.randomUUID()));
  }
}
