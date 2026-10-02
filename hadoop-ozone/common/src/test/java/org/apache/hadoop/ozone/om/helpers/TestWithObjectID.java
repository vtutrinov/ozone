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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.om.helpers.WithObjectID.RAFT_GROUP_SERIAL_SHIFT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the monotonic updateID check of {@link WithObjectID}.
 */
class TestWithObjectID {

  private static final long GROUP_1 = 1L << RAFT_GROUP_SERIAL_SHIFT;
  private static final long GROUP_2 = 2L << RAFT_GROUP_SERIAL_SHIFT;

  @AfterEach
  void tearDown() {
    WithObjectID.setUpdateIdCheckPerRaftGroup(false);
  }

  private static OmVolumeArgs volumeWithUpdateID(long updateID) {
    return new OmVolumeArgs.Builder()
        .setVolume("vol1")
        .setAdminName("admin")
        .setOwnerName("owner")
        .setObjectID(1L)
        .setUpdateID(updateID)
        .build();
  }

  private static void update(long from, long to) {
    volumeWithUpdateID(from).toBuilder().setUpdateID(to).build();
  }

  @Test
  void updateIdMustNotDecrease() {
    assertDoesNotThrow(() -> update(5, 5));
    assertDoesNotThrow(() -> update(5, 6));
    assertThrows(IllegalArgumentException.class, () -> update(6, 5));
    assertThrows(IllegalArgumentException.class, () -> update(GROUP_1 + 1, 5));
  }

  @Test
  void updateIdMustNotDecreaseWithinRaftGroup() {
    WithObjectID.setUpdateIdCheckPerRaftGroup(true);
    // same raft group: strict
    assertThrows(IllegalArgumentException.class, () -> update(6, 5));
    assertThrows(IllegalArgumentException.class, () -> update(GROUP_1 + 6, GROUP_1 + 5));
    assertDoesNotThrow(() -> update(GROUP_1 + 5, GROUP_1 + 6));
    // updates by different raft groups are not ordered
    assertDoesNotThrow(() -> update(GROUP_1 + 6, 5));
    assertDoesNotThrow(() -> update(GROUP_2 + 6, GROUP_1 + 5));
    assertDoesNotThrow(() -> update(5, GROUP_2 + 1));
  }
}
