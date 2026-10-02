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

package org.apache.hadoop.ozone.om.request.group;

import static org.apache.hadoop.ozone.om.OmRaftGroupManager.MAX_BUCKET_RAFT_GROUP_SERIAL_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmRaftGroupManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMCreateRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRaftGroupsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles create raft group request.
 */
public class OMCreateRaftGroupsRequest extends OMClientRequest {

  public static final Logger LOG = LoggerFactory.getLogger(OMCreateRaftGroupsRequest.class);

  public OMCreateRaftGroupsRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMRequest omRequest = getOmRequest();
    CreateBucketRaftGroupsRequest createBucketRaftGroupsRequest = omRequest.getCreateBucketRaftGroupsRequest();
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(omRequest);
    final List<UUID> groupIds = createBucketRaftGroupsRequest.getGroupIdsList().stream()
        .map(HddsUtils::fromProtobuf)
        .collect(Collectors.toList());
    final long maxSerial;
    try {
      maxSerial = validateSerials(ozoneManager.getOmRaftGroupManager().getMaxBucketRaftGroupSerial(), groupIds);
    } catch (IOException e) {
      LOG.warn("Rejected creating bucket raft groups {}: {}", groupIds, e.getMessage());
      return new OMCreateRaftGroupsResponse(createErrorOMResponse(omResponse, e), -1);
    }

    if (createBucketRaftGroupsRequest.getPurgeExistingRaftGroups()) {
      try {
        Iterable<RaftGroup> existingRaftGroups = ozoneManager.getOmRatisServer().getServer().getGroups();
        for (RaftGroup group : existingRaftGroups) {
          if (!group.getGroupId().equals(ozoneManager.getOmRatisServer().getCurrentRaftGroupId())) {
            ozoneManager.getOmRatisServer().removeBucketRaftGroup(group.getGroupId());
          }
        }
      } catch (IOException e) {
        LOG.warn("Something went wrong on deleting existing raft groups", e);
      }
    }
    // the serials are allocated before the groups are created: a failed creation does not make them reusable
    ozoneManager.getMetadataManager().getMultiRaftInfoTable().addCacheEntry(
        new CacheKey<>(MAX_BUCKET_RAFT_GROUP_SERIAL_KEY), CacheValue.get(context.getIndex(), maxSerial));
    groupIds.forEach(groupId -> {
      ozoneManager.createRaftGroupForBucket(RaftGroupId.valueOf(groupId));
      ozoneManager.getOmRaftGroupManager().addGroupIdToRaftGroupCounter(groupId);
    });

    CreateBucketRaftGroupsResponse createBucketRaftGroupsResponse =
            CreateBucketRaftGroupsResponse.newBuilder().build();
    omResponse.setCreateBucketRaftGroupsResponse(createBucketRaftGroupsResponse);
    return new OMCreateRaftGroupsResponse(omResponse.build(), maxSerial);
  }

  /**
   * The ids of new bucket raft groups must carry serials above every serial allocated before, in ascending order:
   * a group id, and with it the object IDs its transactions generate, is never reused.
   * @return the new highest serial
   */
  public static long validateSerials(long maxSerial, List<UUID> groupIds) throws OMException {
    long max = maxSerial;
    for (UUID groupId : groupIds) {
      final long serial = OmRaftGroupManager.getBucketRaftGroupSerial(groupId);
      if (serial < 0) {
        throw new OMException("Not a bucket raft group id: " + groupId, INVALID_REQUEST);
      }
      if (serial <= max) {
        throw new OMException("Serial " + serial + " of bucket raft group " + groupId
            + " is not above the highest serial allocated " + max, INVALID_REQUEST);
      }
      max = serial;
    }
    return max;
  }
}
