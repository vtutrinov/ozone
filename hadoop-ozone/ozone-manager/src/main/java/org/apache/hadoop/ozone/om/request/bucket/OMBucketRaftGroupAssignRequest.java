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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketRaftGroupAssignResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SDP (SDPOZN-1979, multi-raft): assigns a raft group to a bucket. Submitted to the main OM raft group, so that all
 * OMs agree on the bucket to raft group mapping.
 */
public class OMBucketRaftGroupAssignRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMBucketRaftGroupAssignRequest.class);

  public OMBucketRaftGroupAssignRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final BucketRaftGroupAssignRequest assignRequest = getOmRequest().getBucketRaftGroupAssignRequest();
    final String bucketPath = assignRequest.getBucketPath();
    final UUID requestedRaftGroup = HddsUtils.fromProtobuf(assignRequest.getRaftGroupId());
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    final OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    LOG.info("MULTIRAFT: assign raft group: {} to bucket: {}, trxId: {}", requestedRaftGroup, bucketPath,
        context.getIndex());

    OmBucketInfo bucketInfo;
    try {
      bucketInfo = metadataManager.getBucketTable().get(bucketPath);
    } catch (IOException e) {
      LOG.error("Failed to read bucket {}", bucketPath, e);
      return new OMBucketRaftGroupAssignResponse(omResponse.setSuccess(false)
          .setStatus(Status.INTERNAL_ERROR).setMessage(e.getMessage()).build(), null);
    }
    if (bucketInfo == null) {
      return new OMBucketRaftGroupAssignResponse(omResponse.setSuccess(false)
          .setStatus(Status.BUCKET_NOT_FOUND).setMessage("Bucket not found: " + bucketPath).build(), null);
    }

    final String volumeName = bucketInfo.getVolumeName();
    final String bucketName = bucketInfo.getBucketName();
    // bucket raft groups update the bucket (e.g. usedBytes) concurrently, under the bucket lock
    mergeOmLockDetails(metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
    final OmBucketInfo updated;
    final UUID raftGroupUUID;
    try {
      // re-read under the lock: the bucket table is fully cached
      final CacheValue<OmBucketInfo> current =
          metadataManager.getBucketTable().getCacheValue(new CacheKey<>(bucketPath));
      if (current == null || current.getCacheValue() == null) {
        return new OMBucketRaftGroupAssignResponse(omResponse.setSuccess(false)
            .setStatus(Status.BUCKET_NOT_FOUND).setMessage("Bucket not found: " + bucketPath).build(), null);
      }
      raftGroupUUID = ozoneManager.getOmRaftGroupManager().defineRaftGroupForBucket(bucketPath, requestedRaftGroup);
      updated = current.getCacheValue().toBuilder().setRaftGroup(raftGroupUUID).build();
      metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketPath),
          CacheValue.get(context.getIndex(), updated));
    } finally {
      mergeOmLockDetails(metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
    }

    omResponse.setBucketRaftGroupAssignResponse(BucketRaftGroupAssignResponse.newBuilder()
        .setBucketPath(bucketPath)
        .setRaftGroupId(HddsUtils.toProtobuf(raftGroupUUID)));
    return new OMBucketRaftGroupAssignResponse(omResponse.build(), updated.copyObject());
  }
}
