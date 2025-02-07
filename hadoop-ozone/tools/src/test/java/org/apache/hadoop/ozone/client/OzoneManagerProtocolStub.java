/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

/**
 * OzoneManagerClient in-memory stub for testing.
 */
public class OzoneManagerProtocolStub implements OzoneManagerProtocol {
  private final Map<String, OmVolumeArgs> volumes = new HashMap<>();

  private final Map<String, OmBucketInfo> buckets = new HashMap<>();

  private final Map<String, OmKeyInfo> keys = new HashMap<>();

  private final AtomicLong sessionId = new AtomicLong(0);

  public OzoneManagerProtocolStub() throws IOException {
  }

  @Override
  public void createVolume(OmVolumeArgs volumeArgs) {
    String volumeName = volumeArgs.getVolume();

    volumes.put(volumeName, volumeArgs);
  }

  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws OMException {
    if (!volumes.containsKey(volume)) {
      throw new OMException(format("Volume '%s' is missing", volume), VOLUME_NOT_FOUND);
    }

    return volumes.get(volume);
  }

  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String prevKey, int maxKeys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey, int maxKeys) {
    return new ArrayList<>(volumes.values());
  }

  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();

    buckets.put(fullBucketName(volumeName, bucketName), bucketInfo);
  }

  @Override
  public OmBucketInfo getBucketInfo(String volumeName, String bucketName) throws OMException {
    if (!buckets.containsKey(fullBucketName(volumeName, bucketName))) {
      throw new OMException(format("Bucket '%s.%s' is missing", volumeName, bucketName), BUCKET_NOT_FOUND);
    }

    return buckets.get(fullBucketName(volumeName, bucketName));
  }

  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    BlockID dummyBlockID = new BlockID(1L, 1L);
    DatanodeDetails.Port standalonePort = DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 12346);

    DatanodeDetails dummyDatanode = DatanodeDetails.newBuilder()
            .setHostName("dummy-host")
            .setIpAddress("127.0.0.1")
            .setUuid(UUID.randomUUID())
            .addPort(standalonePort)
            .build();

    List<DatanodeDetails> dummyNodes = new ArrayList<>();

    dummyNodes.add(dummyDatanode);

    ReplicationConfig replicationConfig = StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

    PipelineID dummyPipelineId = PipelineID.randomId();

    Pipeline.PipelineState dummyState = Pipeline.PipelineState.OPEN;

    Pipeline dummyPipeline = Pipeline.newBuilder()
            .setState(dummyState)
            .setReplicationConfig(replicationConfig)
            .setId(dummyPipelineId)
            .setNodes(dummyNodes)
            .build();

    long dummyLength = 1024L;
    long dummyOffset = 0L;
    Token<OzoneBlockTokenIdentifier> dummyToken = null;
    int dummyPartNumber = 1;
    long dummyCreateVersion = 1L;
    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
            .setBlockID(dummyBlockID)
            .setPipeline(dummyPipeline)
            .setLength(dummyLength)
            .setOffset(dummyOffset)
            .setToken(dummyToken)
            .setPartNumber(dummyPartNumber)
            .setCreateVersion(dummyCreateVersion)
            .build();

    List<OmKeyLocationInfo> locations = new ArrayList<>();
    locations.add(locationInfo);
    long version = 1L;
    OmKeyLocationInfoGroup locationGroup = new OmKeyLocationInfoGroup(version, locations);

    List<OmKeyLocationInfoGroup> locationInfoGroups = new ArrayList<>();
    locationInfoGroups.add(locationGroup);

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
            .setVolumeName(args.getVolumeName())
            .setBucketName(args.getBucketName())
            .setKeyName(args.getKeyName())
            .setCreationTime(System.currentTimeMillis())
            .setModificationTime(System.currentTimeMillis())
            .setDataSize(args.getDataSize())
            .setFile(true)
            .setOmKeyLocationInfos(locationInfoGroups)
            .build();

    keys.put(fullKeyName(args.getVolumeName(), args.getBucketName(), args.getKeyName()), keyInfo);

    return new OpenKeySession(sessionId.incrementAndGet(), keyInfo, 0);
  }

  public OpenKeySession openCorruptedKey(OmKeyArgs args) throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
            .setVolumeName(args.getVolumeName())
            .setBucketName(args.getBucketName())
            .setKeyName(args.getKeyName())
            .setCreationTime(System.currentTimeMillis())
            .setModificationTime(System.currentTimeMillis())
            .setDataSize(args.getDataSize())
            .setFile(true)
            .build();

    keys.put(fullKeyName(args.getVolumeName(), args.getBucketName(), args.getKeyName()), keyInfo);

    return new OpenKeySession(sessionId.incrementAndGet(), keyInfo, 0);
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public Map<String, OmKeyInfo> getKeys() {
    return keys;
  }

  public void deleteKey(OmKeyArgs args) throws IOException {
    keys.remove(fullKeyName(args.getVolumeName(), args.getBucketName(), args.getKeyName()));
  }

  @Override
  public KeyInfoWithVolumeContext getKeyInfo(OmKeyArgs args, boolean assumeS3Context) throws IOException {
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();

    String fullKeyName = fullKeyName(volumeName, bucketName, keyName);

    if (!keys.containsKey(fullKeyName)) {
      throw new OMException(format("Key '%s.%s.%s' is missing", volumeName, bucketName, keyName), KEY_NOT_FOUND);
    }

    OmKeyInfo keyInfo = keys.get(fullKeyName);

    OmVolumeArgs volumeArgs = volumes.get(args.getVolumeName());

    return KeyInfoWithVolumeContext.newBuilder()
            .setKeyInfo(keyInfo)
            .setVolumeArgs(volumeArgs)
            .setUserPrincipal(UserGroupInformation.getCurrentUser().getShortUserName())
            .build();
  }

  @Override
  public List<OmBucketInfo> listBuckets(String volumeName, String startBucketName, String bucketPrefix,
                                        int maxNumOfBuckets, boolean hasSnapshot) {
    return new ArrayList<>(buckets.values());
  }

  @Override
  public List<ServiceInfo> getServiceList() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ServiceInfoEx getServiceInfo() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void transferLeadership(String newLeaderId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean triggerRangerBGSync(boolean noWait) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(String upgradeClientID, boolean takeover,
                                                                             boolean readonly) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName, String bucketName, String keyName, String uploadID,
                                              int partNumberMarker, int maxParts) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName, String bucketName, String prefix) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public S3VolumeContext getS3VolumeContext() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public TenantStateList listTenant() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs keyArgs) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ListKeysResult listKeys(String volumeName, String bucketName, String startKey,
      String keyPrefix, int maxKeys) {
    return new ListKeysResult(new ArrayList<>(keys.values()), false);
  }

  @Override
  public ListKeysLightResult listKeysLight(String volumeName, String bucketName, String startKey, String keyPrefix,
                                           int maxKeys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive, String startKey, long numEntries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs keyArgs) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive, String startKey, long numEntries,
                                          boolean allowPartialPrefixes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(OmKeyArgs keyArgs, boolean recursive, String startKey,
                                                    long numEntries, boolean allowPartialPrefixes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DBUpdates getDBUpdates(OzoneManagerProtocolProtos.DBUpdatesRequest dbUpdatesRequest) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName, String startKeyName,
      String keyPrefix, int maxKeys) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneManagerProtocolProtos.EchoRPCResponse echoRPCReq(byte[] payloadReq, int payloadSizeResp,
                                                               boolean writeToRatis) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean recoverLease(String volumeName, String bucketName, String keyName) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setTimes(OmKeyArgs keyArgs, long mtime, long atime) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public UUID refetchSecretKey() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private static String fullBucketName(String volumeName, String bucketName) {
    return volumeName + "/" + bucketName;
  }

  private static String fullKeyName(String volumeName, String bucketName, String key) {
    return volumeName + "/" + bucketName + "/" + key;
  }
}
