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

package org.apache.hadoop.ozone.client;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.storage.ContainerStorageStub;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.mockito.Mockito;

/**
 * ClientProtocol implementation with in-memory state.
 */
public abstract class ClientProtocolStub implements ClientProtocol {
  /** Creates the stub; interface methods it doesn't implement return defaults. */
  public static ClientProtocolStub create() {
    return Mockito.mock(ClientProtocolStub.class, Mockito.withSettings()
        .useConstructor()
        .defaultAnswer(Mockito.CALLS_REAL_METHODS));
  }

  private static final String STUB_KERBEROS_ID = "stub_kerberos_id";

  private static final String STUB_SECRET = "stub_secret";

  private static final String USER_ROOT = "root";

  private final OzoneManagerProtocolStub ozoneManagerProtocol;

  private final ContainerStorageStub containerStorageStub = new ContainerStorageStub();

  public ClientProtocolStub() throws IOException {
    this.ozoneManagerProtocol = OzoneManagerProtocolStub.create();
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
    VolumeArgs defaultArgs = VolumeArgs.newBuilder()
            .setAdmin(USER_ROOT)
            .setOwner(USER_ROOT)
            .setQuotaInBytes(Integer.MAX_VALUE)
            .build();

    createVolume(volumeName, defaultArgs);
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs args)
          throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName(args.getAdmin())
            .setOwnerName(args.getOwner())
            .setQuotaInBytes(args.getQuotaInBytes())
            .setQuotaInNamespace(args.getQuotaInNamespace())
            .setCreationTime(Time.now())
            .build();

    ozoneManagerProtocol.createVolume(volumeArgs);
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = ozoneManagerProtocol.getVolumeInfo(volumeName);

    return createVolumeObject(volumeArgs);
  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume, int maxListResult) throws IOException {
    return listVolumes(UserGroupInformation.getCurrentUser().getUserName(), volumePrefix, prevVolume, maxListResult);
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix, String prevVolume, int maxListResult)
          throws IOException {
    return ozoneManagerProtocol.listAllVolumes(volumePrefix, prevVolume, maxListResult)
            .stream()
            .map(this::createVolumeObject)
            .collect(toList());
  }

  private OzoneVolume createVolumeObject(OmVolumeArgs volumeArgs) {
    return OzoneVolumeStub.newBuilder(this)
            .setName(volumeArgs.getVolume())
            .setOwner(volumeArgs.getOwnerName())
            .setAdmin(volumeArgs.getAdminName())
            .setAcls(volumeArgs.getAcls())
            .setCreationTime(volumeArgs.getCreationTime())
            .setModificationTime(volumeArgs.getModificationTime())
            .setQuotaInBytes(volumeArgs.getQuotaInBytes())
            .setMetadata(volumeArgs.getMetadata())
            .setRefCount(volumeArgs.getRefCount())
            .setUsedNamespace(volumeArgs.getUsedNamespace())
            .build();
  }

  @Override
  public void createBucket(String volumeName, String bucketName) throws IOException {
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setStorageType(StorageType.DEFAULT)
            .setVersioning(false)
            .build();

    createBucket(volumeName, bucketName, bucketArgs);
  }

  @Override
  public void createBucket(String volumeName, String bucketName, BucketArgs bucketArgs) throws IOException {
    DefaultReplicationConfig defaultReplicationConfig =
            new DefaultReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setDefaultReplicationConfig(defaultReplicationConfig)
            .setBucketLayout(bucketArgs.getBucketLayout())
            .setStorageType(bucketArgs.getStorageType())
            .setIsVersionEnabled(bucketArgs.getVersioning())
            .setCreationTime(Time.now())
            .build();

    ozoneManagerProtocol.createBucket(bucketInfo);
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName) throws IOException {
    OmBucketInfo bucketInfo = ozoneManagerProtocol.getBucketInfo(volumeName, bucketName);

    return createBucketObject(bucketInfo);
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix, String prevBucket,
                                       int maxListResult, boolean hasSnapshot) throws IOException {
    return ozoneManagerProtocol.listBuckets(volumeName, null, null, maxListResult, hasSnapshot)
            .stream()
            .map(this::createBucketObject)
            .collect(toList());
  }

  private OzoneBucket createBucketObject(OmBucketInfo bucketInfo) {
    return OzoneBucketStub.newBuilder(this)
            .setVolumeName(bucketInfo.getVolumeName())
            .setName(bucketInfo.getBucketName())
            .setStorageType(bucketInfo.getStorageType())
            .setSourceBucket(bucketInfo.getSourceBucket())
            .setBucketLayout(bucketInfo.getBucketLayout())
            .setCreationTime(bucketInfo.getCreationTime())
            .setModificationTime(bucketInfo.getModificationTime())
            .setMetadata(bucketInfo.getMetadata())
            .setDefaultReplicationConfig(bucketInfo.getDefaultReplicationConfig())
            .setEncryptionKeyName(Optional.ofNullable(bucketInfo.getEncryptionKeyInfo())
                    .map(BucketEncryptionKeyInfo::getKeyName)
                    .orElse(null))
            .setVersioning(bucketInfo.getIsVersionEnabled())
            .setUsedNamespace(bucketInfo.getUsedNamespace())
            .setQuotaInBytes(bucketInfo.getQuotaInBytes())
            .setQuotaInNamespace(bucketInfo.getQuotaInNamespace())
            .setUsedBytes(bucketInfo.getUsedBytes())
            .build();
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, Map<String, String> metadata) throws IOException {
    return createKey(
            volumeName,
            bucketName,
            keyName,
            size,
            ReplicationConfig.fromTypeAndFactor(type, factor),
            metadata
    );
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
                                     ReplicationConfig replicationConfig, Map<String,
                                     String> metadata) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDataSize(size)
            .setReplicationConfig(replicationConfig)
            .setLocationInfoList(new ArrayList<>())
            .addAllMetadataGdpr(metadata)
            .setAcls(emptyList())
            .setLatestVersionLocation(false)
            .build();

    OpenKeySession openKey = ozoneManagerProtocol.openKey(keyArgs);

    return createOutputStream(openKey);
  }

  public OzoneOutputStream createCorruptedKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, Map<String, String> metadata) throws IOException {
    return createCorruptedKey(
            volumeName,
            bucketName,
            keyName,
            size,
            ReplicationConfig.fromTypeAndFactor(type, factor),
            metadata
    );
  }

  public OzoneOutputStream createCorruptedKey(String volumeName, String bucketName, String keyName, long size,
                                              ReplicationConfig replicationConfig, Map<String,
                                              String> metadata) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDataSize(size)
            .setReplicationConfig(replicationConfig)
            .setLocationInfoList(new ArrayList<>())
            .addAllMetadataGdpr(metadata)
            .setAcls(emptyList())
            .setLatestVersionLocation(false)
            .build();

    OpenKeySession openKey = ozoneManagerProtocol.openCorruptedKey(keyArgs);

    return createOutputStream(openKey);
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey) {
    ByteArrayOutputStream byteArrayOutputStream = new OutputStreamStub(openKey,  containerStorageStub);

    return new OzoneOutputStream(byteArrayOutputStream, null);
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName, String keyPrefix, String prevKey,
                                 int maxListResult) throws IOException {
    return ozoneManagerProtocol.listKeys(volumeName, bucketName, null, keyPrefix, maxListResult)
            .getKeys()
            .stream()
            .map(OzoneKey::fromKeyInfo)
            .collect(toList());
  }

  @Override
  @Nonnull
  public S3SecretValue getS3Secret(String kerberosID) {
    return S3SecretValue.of(STUB_KERBEROS_ID, STUB_SECRET);
  }

  @Override
  public OzoneManagerProtocol getOzoneManagerClient() {
    return ozoneManagerProtocol;
  }

  @Override
  public BucketRaftGroupAssignResponse assignBucketRaftGroup(BucketRaftGroupAssignRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void acquireBucketRaftGroupAssignmentWriteLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseBucketRaftGroupAssignmentWriteLock() {
    throw new UnsupportedOperationException();
  }
}
