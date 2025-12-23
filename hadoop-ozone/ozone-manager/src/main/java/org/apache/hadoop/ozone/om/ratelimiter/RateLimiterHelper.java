/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.ratelimiter;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.util.EnumSet;

/**
 * Helper class for rate limiters.
 */
public final class RateLimiterHelper {
  private RateLimiterHelper() {
  }

  public static final EnumSet<OzoneManagerProtocolProtos.Type> RATE_LIMITED_READ_CMDS = EnumSet.of(
          Type.LookupKey,
          Type.ListKeys,
          Type.ListKeysLight,
          Type.GetFileStatus,
          Type.LookupFile,
          Type.ListStatus,
          Type.ListStatusLight,
          Type.GetKeyInfo,
          Type.InfoBucket,
          Type.ListBuckets,
          Type.ListTrash,
          Type.ListMultiPartUploadParts,
          Type.ListMultipartUploads,
          Type.CancelSnapshotDiff,
          Type.ListSnapshot,
          Type.SnapshotDiff,
          Type.ListSnapshotDiffJobs,
          Type.GetSnapshotInfo,
          Type.GetContentSummary
  );

  public static final EnumSet<OzoneManagerProtocolProtos.Type> RATE_LIMITED_WRITE_CMDS = EnumSet.of(
          Type.CreateKey,
          Type.DeleteKey,
          Type.CreateFile,
          Type.DeleteKeys,
          Type.CreateBucket,
          Type.DeleteBucket,
          Type.SetBucketProperty,
          Type.PurgeKeys,
          Type.CreateSnapshot,
          Type.DeleteSnapshot,
          Type.RenameSnapshot,
          Type.RecoverLease,
          Type.CreateDirectory,
          Type.AllocateBlock,
          Type.CommitKey,
          Type.RenameKey,
          Type.RenameKeys,
          Type.InitiateMultiPartUpload,
          Type.CommitMultiPartUpload,
          Type.AbortMultiPartUpload,
          Type.CompleteMultiPartUpload,
          Type.SetTimes
  );

  public static String toDBKey(OMMetadataManager metadataManager,
                             String volume,
                             String bucket,
                             OzoneManagerProtocolProtos.RateLimiterType type) {
    String bucketKey = metadataManager.getBucketKey(volume, bucket);
    return bucketKey + ":" + type.name();
  }
}
