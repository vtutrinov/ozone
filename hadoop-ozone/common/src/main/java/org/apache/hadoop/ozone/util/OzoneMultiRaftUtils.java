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

package org.apache.hadoop.ozone.util;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneMultiRaftUtils {

  private OzoneMultiRaftUtils() {
  }

  @SuppressWarnings("checkstyle:methodlength")
  public static String getBucketName(OzoneManagerProtocolProtos.OMRequest omRequest) {

    // Handling of exception by createClientRequest(OMRequest, OzoneManger):
    // Either the code will take FSO or non FSO path, both classes has a
    // validateAndUpdateCache() function which also contains
    // validateBucketAndVolume() function which validates bucket and volume and
    // throws necessary exceptions if required. validateAndUpdateCache()
    // function has catch block which catches the exception if required and
    // handles it appropriately.
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    switch (cmdType) {
    case RecoverLease:
      return omRequest.getRecoverLeaseRequest().getBucketName();
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
          omRequest.getDeleteKeysRequest()
              .getDeleteKeys();
      return deleteKeyArgs.getBucketName();
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
          omRequest.getRenameKeysRequest().getRenameKeysArgs();
      return renameKeysArgs.getBucketName();
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case SetTimes:
      keyArgs = omRequest.getSetTimesRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case PutObjectTagging:
      keyArgs = omRequest.getPutObjectTaggingRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case DeleteObjectTagging:
      keyArgs = omRequest.getDeleteObjectTaggingRequest().getKeyArgs();
      return keyArgs.getBucketName();
    default:
      return null;
    }
  }

  @SuppressWarnings("checkstyle:methodlength")
  public static String getVolumeName(OzoneManagerProtocolProtos.OMRequest omRequest) {

    // Handling of exception by createClientRequest(OMRequest, OzoneManger):
    // Either the code will take FSO or non FSO path, both classes has a
    // validateAndUpdateCache() function which also contains
    // validateBucketAndVolume() function which validates bucket and volume and
    // throws necessary exceptions if required. validateAndUpdateCache()
    // function has catch block which catches the exception if required and
    // handles it appropriately.
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    switch (cmdType) {
    case RecoverLease:
      return omRequest.getRecoverLeaseRequest().getVolumeName();
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
          omRequest.getDeleteKeysRequest()
              .getDeleteKeys();
      return deleteKeyArgs.getVolumeName();
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
          omRequest.getRenameKeysRequest().getRenameKeysArgs();
      return renameKeysArgs.getVolumeName();
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case SetTimes:
      keyArgs = omRequest.getSetTimesRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case PutObjectTagging:
      keyArgs = omRequest.getPutObjectTaggingRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    case DeleteObjectTagging:
      keyArgs = omRequest.getDeleteObjectTaggingRequest().getKeyArgs();
      return keyArgs.getVolumeName();
    default:
      return null;
    }
  }

  /**
   * The volume and bucket of a read request served from the state of a bucket raft group (keys, files, multipart
   * uploads), so that its consistency is checked against that raft group.
   * @return {volume, bucket}, or null for other requests
   */
  public static String[] getReadRequestBucket(OzoneManagerProtocolProtos.OMRequest omRequest) {
    final OzoneManagerProtocolProtos.KeyArgs keyArgs;
    switch (omRequest.getCmdType()) {
    case LookupKey:
      keyArgs = omRequest.getLookupKeyRequest().getKeyArgs();
      break;
    case GetKeyInfo:
      keyArgs = omRequest.getGetKeyInfoRequest().getKeyArgs();
      break;
    case LookupFile:
      keyArgs = omRequest.getLookupFileRequest().getKeyArgs();
      break;
    case GetFileStatus:
      keyArgs = omRequest.getGetFileStatusRequest().getKeyArgs();
      break;
    case ListStatus:
    case ListStatusLight:
      keyArgs = omRequest.getListStatusRequest().getKeyArgs();
      break;
    case GetObjectTagging:
      keyArgs = omRequest.getGetObjectTaggingRequest().getKeyArgs();
      break;
    case ListKeys:
    case ListKeysLight:
      return bucketOrNull(omRequest.getListKeysRequest().getVolumeName(),
          omRequest.getListKeysRequest().getBucketName());
    case ListMultiPartUploadParts:
      return bucketOrNull(omRequest.getListMultipartUploadPartsRequest().getVolume(),
          omRequest.getListMultipartUploadPartsRequest().getBucket());
    case ListMultipartUploads:
      return bucketOrNull(omRequest.getListMultipartUploadsRequest().getVolume(),
          omRequest.getListMultipartUploadsRequest().getBucket());
    default:
      return null;
    }
    return bucketOrNull(keyArgs.getVolumeName(), keyArgs.getBucketName());
  }

  private static String[] bucketOrNull(String volumeName, String bucketName) {
    return volumeName == null || volumeName.isEmpty() || bucketName == null || bucketName.isEmpty()
        ? null : new String[] {volumeName, bucketName};
  }
}
