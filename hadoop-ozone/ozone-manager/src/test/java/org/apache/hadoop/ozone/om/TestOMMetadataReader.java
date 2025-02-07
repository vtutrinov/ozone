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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.ContentSummary;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;


/**
 * Test ozone metadata reader.
 */
public class TestOMMetadataReader {

  @Test
  public void testGetClientAddress() {
    try (
        MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class);
        MockedStatic<Context> grpcRequestContextStaticMock = mockStatic(Context.class);
    ) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      Context.Key<String> clientIpAddressKey = mock(Context.Key.class);
      when(clientIpAddressKey.get())
          .thenReturn(expectedClientAddressInCaseOfGrpcCall, null);

      grpcRequestContextStaticMock.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(clientIpAddressKey);

      // when (GRPC call with defined client address)
      String clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfGrpcCall, clientAddress);

      // and when (GRPC call without client address)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals("", clientAddress);

      // and when (Hadoop RPC client call)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfHadoopRpcCall, clientAddress);
    }
  }

  @Test
  public void testGetContentSummaryFor5EmptyVolumes() throws IOException {
    // given
    List<OmVolumeArgs> listOf5Volumes = listOfVolumesOfSize(5);
    OzoneManager om = mock(OzoneManager.class);

    OzoneConfiguration configuration = mock(OzoneConfiguration.class);
    when(om.getConfiguration()).thenReturn(configuration);
    when(configuration.getInt(anyString(), anyInt())).thenReturn(1000);

    when(om.listVolumeByUser(any(), any(), any(), anyInt()))
        .thenReturn(listOf5Volumes)
        .thenReturn(Collections.emptyList());

    BucketManager bucketManager = mock(BucketManager.class);
    when(om.getBucketManager()).thenReturn(bucketManager);

    VolumeManager volumeManager = mock(VolumeManager.class);
    when(om.getVolumeManager()).thenReturn(volumeManager);

    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(om.getPerfMetrics()).thenReturn(omPerformanceMetrics);

    KeyManager keyManager = mock(KeyManager.class);
    PrefixManager prefixManager = mock(PrefixManager.class);
    OmMetadataReaderMetrics omMetadataReaderMetrics = mock(OmMetadataReaderMetrics.class);
    OmMetadataReader omMetadataReader = spy(new OmMetadataReader(keyManager, prefixManager, om,
        LoggerFactory.getLogger(OmMetadataReader.class), mock(AuditLogger.class), omMetadataReaderMetrics,
        null));

    // when
    OmKeyArgs omKeyArgs = spy((new OmKeyArgs.Builder().setVolumeName("").setBucketName("").build()));
    ContentSummary contentSummary = omMetadataReader.getContentSummary(omKeyArgs, "ozone");

    // then
    assertEquals(0, contentSummary.getFileCount());
    // 5 volumes + 1 root directory
    assertEquals(6, contentSummary.getDirectoryCount());
    assertEquals(0, contentSummary.getLength());
  }

  @Test
  public void testGetContentSummaryFor10EmptyBucketsWithin5Volumes() throws IOException {
    // given
    List<OmVolumeArgs> listOf5Volumes = listOfVolumesOfSize(5);
    OzoneManager om = mock(OzoneManager.class);

    OzoneConfiguration configuration = mock(OzoneConfiguration.class);
    when(om.getConfiguration()).thenReturn(configuration);
    when(configuration.getInt(anyString(), anyInt())).thenReturn(1000);

    when(om.listVolumeByUser(any(), any(), any(), anyInt()))
        .thenReturn(listOf5Volumes)
        .thenReturn(Collections.emptyList());

    BucketManager bucketManager = mock(BucketManager.class);
    when(om.getBucketManager()).thenReturn(bucketManager);
    // 2 buckets per volume: 2 * 5 = 10 buckets
    mockBucketsListPerVolume(bucketManager, listOf5Volumes, 2);


    VolumeManager volumeManager = mock(VolumeManager.class);
    when(om.getVolumeManager()).thenReturn(volumeManager);

    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(om.getPerfMetrics()).thenReturn(omPerformanceMetrics);

    KeyManager keyManager = mock(KeyManager.class);
    PrefixManager prefixManager = mock(PrefixManager.class);
    OmMetadataReaderMetrics omMetadataReaderMetrics = mock(OmMetadataReaderMetrics.class);
    ReferenceCounted<IOmMetadataReader> omMetadataReaderReferenceCounted = mock(ReferenceCounted.class);
    when(om.getReader(any(OmKeyArgs.class))).thenReturn(omMetadataReaderReferenceCounted);
    IOmMetadataReader fileStatusesReader = mock(IOmMetadataReader.class);
    when(omMetadataReaderReferenceCounted.get()).thenReturn(fileStatusesReader);
    doAnswer(invocation -> Collections.emptyList()).when(fileStatusesReader).listStatusLight(any(), anyBoolean(),
        anyString(), anyInt(), anyBoolean());
    OmMetadataReader omMetadataReader = new OmMetadataReader(keyManager, prefixManager, om,
        LoggerFactory.getLogger(OmMetadataReader.class), mock(AuditLogger.class), omMetadataReaderMetrics,
        null);

    // when
    ContentSummary contentSummary = omMetadataReader.getContentSummary(new OmKeyArgs.Builder().setVolumeName("")
        .setBucketName("").build(), "ozone");

    // then
    assertEquals(0, contentSummary.getFileCount());
    assertEquals(16, contentSummary.getDirectoryCount()); // 5 volumes + 1 root directory + 10 buckets
    assertEquals(0, contentSummary.getLength());
  }

  @Test
  public void testGetContentSummaryFor100TwoKilobytesKeysWithin10BucketsWithin5Volumes() throws IOException {
    // given
    List<OmVolumeArgs> listOf5Volumes = listOfVolumesOfSize(5);
    OzoneManager om = mock(OzoneManager.class);

    OzoneConfiguration configuration = mock(OzoneConfiguration.class);
    when(om.getConfiguration()).thenReturn(configuration);
    when(configuration.getInt(anyString(), anyInt())).thenReturn(1000);

    when(om.listVolumeByUser(any(), any(), any(), anyInt()))
        .thenReturn(listOf5Volumes)
        .thenReturn(Collections.emptyList());

    BucketManager bucketManager = mock(BucketManager.class);
    when(om.getBucketManager()).thenReturn(bucketManager);
    // 2 buckets per volume: 2 * 5 = 10 buckets
    List<OmBucketInfo> omBucketInfos = mockBucketsListPerVolume(bucketManager, listOf5Volumes, 2);

    VolumeManager volumeManager = mock(VolumeManager.class);
    when(om.getVolumeManager()).thenReturn(volumeManager);

    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(om.getPerfMetrics()).thenReturn(omPerformanceMetrics);

    KeyManager keyManager = mock(KeyManager.class);
    PrefixManager prefixManager = mock(PrefixManager.class);
    OmMetadataReaderMetrics omMetadataReaderMetrics = mock(OmMetadataReaderMetrics.class);
    ReferenceCounted<IOmMetadataReader> omMetadataReaderReferenceCounted = mock(ReferenceCounted.class);
    when(om.getReader(any(OmKeyArgs.class))).thenReturn(omMetadataReaderReferenceCounted);
    IOmMetadataReader fileStatusesReader = mock(IOmMetadataReader.class);
    when(omMetadataReaderReferenceCounted.get()).thenReturn(fileStatusesReader);
    mockKeyListPerBucket(omBucketInfos, fileStatusesReader, 10); // 10 keys per bucket
    OmMetadataReader omMetadataReader = new OmMetadataReader(keyManager, prefixManager, om,
        LoggerFactory.getLogger(OmMetadataReader.class), mock(AuditLogger.class), omMetadataReaderMetrics,
        null);

    // when
    ContentSummary contentSummary = omMetadataReader.getContentSummary(new OmKeyArgs.Builder().setVolumeName("")
        .setBucketName("").build(), "ozone");

    // then
    assertEquals(100, contentSummary.getFileCount()); // 10 buckets * 10 keys per bucket
    assertEquals(16, contentSummary.getDirectoryCount()); // 5 volumes + 1 root directory + 10 buckets
    assertEquals(204800, contentSummary.getLength()); // 100 keys * 2KB
  }

  /**
   * Test ContentSummary for the structure that has 100 2KB keys within directories (depth of 2 and 10),
   * and directories are within 10 buckets within 5 volumes.
   */
  @Test
  public void testGetContentSummaryForKeysWithinDirectories() throws IOException {
    // given
    List<OmVolumeArgs> listOf5Volumes = listOfVolumesOfSize(5);
    OzoneManager om = mock(OzoneManager.class);

    OzoneConfiguration configuration = mock(OzoneConfiguration.class);
    when(om.getConfiguration()).thenReturn(configuration);
    when(configuration.getInt(anyString(), anyInt())).thenReturn(1000);

    when(om.listVolumeByUser(any(), any(), any(), anyInt()))
        .thenReturn(listOf5Volumes)
        .thenReturn(Collections.emptyList());

    BucketManager bucketManager = mock(BucketManager.class);
    when(om.getBucketManager()).thenReturn(bucketManager);
    // 2 buckets per volume: 2 * 5 = 10 buckets
    List<OmBucketInfo> omBucketInfos = mockBucketsListPerVolume(bucketManager, listOf5Volumes, 2);

    VolumeManager volumeManager = mock(VolumeManager.class);
    when(om.getVolumeManager()).thenReturn(volumeManager);

    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(om.getPerfMetrics()).thenReturn(omPerformanceMetrics);

    KeyManager keyManager = mock(KeyManager.class);
    PrefixManager prefixManager = mock(PrefixManager.class);
    OmMetadataReaderMetrics omMetadataReaderMetrics = mock(OmMetadataReaderMetrics.class);
    ReferenceCounted<IOmMetadataReader> omMetadataReaderReferenceCounted = mock(ReferenceCounted.class);
    when(om.getReader(any(OmKeyArgs.class))).thenReturn(omMetadataReaderReferenceCounted);
    IOmMetadataReader fileStatusesReader = mock(IOmMetadataReader.class);
    when(omMetadataReaderReferenceCounted.get()).thenReturn(fileStatusesReader);
    stubKeysWithinDirectoriesStructure(fileStatusesReader, omBucketInfos);

    // when
    ContentSummary contentSummary = new OmMetadataReader(keyManager, prefixManager, om,
        LoggerFactory.getLogger(OmMetadataReader.class), mock(AuditLogger.class), omMetadataReaderMetrics,
        null).getContentSummary(new OmKeyArgs.Builder().setVolumeName("")
        .setBucketName("").build(), "ozone");

    // then
    // 10 buckets * 15 keys per bucket
    assertEquals(150, contentSummary.getFileCount());
    // 5 volumes + 1 root directory + 10 buckets + 40 directories (4 per bucket)
    assertEquals(56, contentSummary.getDirectoryCount());
    // 150 keys * 2KB
    assertEquals(307200, contentSummary.getLength());
  }

  @Test
  public void testGetContentSummaryForBucketSnapshotDir() throws IOException {
    // when
    List<OmVolumeArgs> listOfSingleVolume = listOfVolumesOfSize(1);
    OzoneManager om = mock(OzoneManager.class);

    OzoneConfiguration configuration = mock(OzoneConfiguration.class);
    when(om.getConfiguration()).thenReturn(configuration);
    when(configuration.getInt(anyString(), anyInt())).thenReturn(1000);

    when(om.listVolumeByUser(any(), any(), any(), anyInt()))
        .thenReturn(listOfSingleVolume)
        .thenReturn(Collections.emptyList());

    BucketManager bucketManager = mock(BucketManager.class);
    when(om.getBucketManager()).thenReturn(bucketManager);
    List<OmBucketInfo> omBucketInfos = mockBucketsListPerVolume(bucketManager, listOfSingleVolume, 1);

    VolumeManager volumeManager = mock(VolumeManager.class);
    when(om.getVolumeManager()).thenReturn(volumeManager);

    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(om.getPerfMetrics()).thenReturn(omPerformanceMetrics);

    KeyManager keyManager = mock(KeyManager.class);
    PrefixManager prefixManager = mock(PrefixManager.class);
    OmMetadataReaderMetrics omMetadataReaderMetrics = mock(OmMetadataReaderMetrics.class);
    ReferenceCounted<IOmMetadataReader> omMetadataReaderReferenceCounted = mock(ReferenceCounted.class);
    when(om.getReader(any(OmKeyArgs.class))).thenReturn(omMetadataReaderReferenceCounted);
    IOmMetadataReader fileStatusesReader = mock(IOmMetadataReader.class);
    when(omMetadataReaderReferenceCounted.get()).thenReturn(fileStatusesReader);

    ListSnapshotResponse<SnapshotInfo> bucketSnapshots = mock(ListSnapshotResponse.class);
    when(bucketSnapshots.getSnapshotInfos()).thenReturn(Collections.singletonList(
        new SnapshotInfo.Builder()
            .setName("snapshot-1")
            .setVolumeName("vol1")
            .setBucketName("bucket1").build()));
    when(bucketSnapshots.getLastSnapshot()).thenReturn(null);
    when(om.listSnapshot(eq("vol1"), eq("bucket1"), eq("snapshot-1"), eq(""), anyInt()))
        .thenReturn(bucketSnapshots)
        .thenReturn(new ListSnapshotResponse<>(Collections.emptyList(), null));

    stubSnapshotForBuckets(fileStatusesReader, omBucketInfos);

    // when
    ContentSummary contentSummary = new OmMetadataReader(keyManager, prefixManager, om,
        LoggerFactory.getLogger(OmMetadataReader.class), mock(AuditLogger.class), omMetadataReaderMetrics,
        null).getContentSummary(new OmKeyArgs.Builder().setVolumeName("vol1")
        .setBucketName("bucket1").setKeyName(".snapshot/snapshot-1").build(), "ozone");

    // then
    assertEquals(4, contentSummary.getFileCount()); // 1 bucket * 4 keys per bucket
    assertEquals(1, contentSummary.getDirectoryCount()); // 1 directory in the snapshot-1
    assertEquals(8192, contentSummary.getLength()); // 4 keys * 2KB
  }

  /**
   * Stub the next list of keys within bucket.
   * @param bucket Ozone bucket that should contain the keys
   * @param desiredKeysForNextIteration list of pairs of (keyName, isDirectory)
   */
  private List<OzoneFileStatusLight> stubTheNextListOfKeyStatuses(OmBucketInfo bucket,
                                            List<Pair<String, Boolean>> desiredKeysForNextIteration) {
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    List<OzoneFileStatusLight> nextListOfKeys = new ArrayList<>(desiredKeysForNextIteration.size());
    for (Pair<String, Boolean> key : desiredKeysForNextIteration) {
      String[] keynameParts = key.getKey().split("/");
      OzoneFileStatusLight ofsl = mock(OzoneFileStatusLight.class);
      when(ofsl.getKeyInfo()).thenReturn(new BasicOmKeyInfo.Builder()
          .setVolumeName(bucket.getVolumeName())
          .setBucketName(bucket.getBucketName())
          .setKeyName(key.getKey())
          .setIsFile(!key.getValue())
          .setReplicationConfig(replicationConfig)
          .setDataSize(!key.getValue() ? 2048 : 0)
          .build());
      when(ofsl.isDirectory()).thenReturn(key.getValue());
      when(ofsl.getTrimmedName()).thenReturn(keynameParts.length > 0
          ? keynameParts[keynameParts.length - 1] : key.getKey());
      when(ofsl.getPath()).thenReturn("/" + bucket.getVolumeName() + "/" + bucket.getBucketName() + "/" + key.getKey());
      when(ofsl.isFile()).thenReturn(!key.getValue());
      when(ofsl.getProtobuf()).thenReturn(null);
      nextListOfKeys.add(ofsl);
    }
    return nextListOfKeys;
  }

  /**
   * Provide the following keys structure within the bucket.
   * k1
   * k2
   * sub
   * sub/dir
   * sub/dir/k3
   * sub/dir/k4
   * sub/dir/k5
   * sub/dir/k6
   * sub/dir/level
   * sub/dir/level/deeper
   * sub/dir/level/deeper/k7
   * sub/dir/level/deeper/k8
   * sub/dir/level/deeper/k9
   * sub/dir/level/deeper/k10
   * sub/dir/level/deeper/k11
   * sub/dir/level/deeper/k12
   * sub/dir/level/deeper/k13
   * sub/dir/level/deeper/k14
   * sub/dir/level/deeper/k15
   *
   * @param fileStatusListProvider Ongoing stub for the listStatusLight method
   * @param omBucketInfos The bucket list which items should contain the keys
   * @throws IOException
   */
  private void stubKeysWithinDirectoriesStructure(IOmMetadataReader fileStatusListProvider,
                                                  List<OmBucketInfo> omBucketInfos) throws IOException {
    List<List<OzoneFileStatusLight>> listOfOFSPerIteration = new ArrayList<>(omBucketInfos.size() * 5);
    for (OmBucketInfo bucket: omBucketInfos) {
      List<OzoneFileStatusLight> list1 = stubTheNextListOfKeyStatuses(bucket, Arrays.asList(
          Pair.of("k1", false), Pair.of("k2", false), Pair.of("sub", true)));
      List<OzoneFileStatusLight> list2 = stubTheNextListOfKeyStatuses(bucket, Collections.singletonList(
          Pair.of("sub/dir", true)));
      List<OzoneFileStatusLight> list3 = stubTheNextListOfKeyStatuses(bucket, Arrays.asList(
          Pair.of("sub/dir/k3", false), Pair.of("sub/dir/k4", false), Pair.of("sub/dir/k5", false),
          Pair.of("sub/dir/k6", false), Pair.of("sub/dir/level", true)));
      List<OzoneFileStatusLight> list4 = stubTheNextListOfKeyStatuses(bucket, Collections.singletonList(
          Pair.of("sub/dir/level/deeper", true)));
      List<OzoneFileStatusLight> list5 = stubTheNextListOfKeyStatuses(bucket, Arrays.asList(
          Pair.of("sub/dir/level/deeper/k7", false), Pair.of("sub/dir/level/deeper/k8", false),
          Pair.of("sub/dir/level/deeper/k9", false), Pair.of("sub/dir/level/deeper/k10", false),
          Pair.of("sub/dir/level/deeper/k11", false), Pair.of("sub/dir/level/deeper/k12", false),
          Pair.of("sub/dir/level/deeper/k13", false), Pair.of("sub/dir/level/deeper/k14", false),
          Pair.of("sub/dir/level/deeper/k15", false)));
      listOfOFSPerIteration.add(list1);
      listOfOFSPerIteration.add(list2);
      listOfOFSPerIteration.add(list3);
      listOfOFSPerIteration.add(list4);
      listOfOFSPerIteration.add(list5);
      listOfOFSPerIteration.add(Collections.singletonList(list5.get(list5.size() - 1)));
    }
    OngoingStubbing<List<OzoneFileStatusLight>> keyStatusListStub = when(fileStatusListProvider.listStatusLight(
        any(OmKeyArgs.class), anyBoolean(), anyString(), anyLong(), anyBoolean()));
    for (List<OzoneFileStatusLight> list: listOfOFSPerIteration) {
      keyStatusListStub = keyStatusListStub.thenReturn(list);
    }
  }

  /**
   * Stub the snapshot structure for the buckets.
   * .snapshot/snapshot-1/key1
   * .snapshot/snapshot-1/key2
   * .snapshot/snapshot-1/dir1
   * .snapshot/snapshot-1/dir1/key3
   * .snapshot/snapshot-1/dir1/key4
   * @param fileStatusListProvider
   * @param omBucketInfos
   * @throws IOException
   */
  private void stubSnapshotForBuckets(IOmMetadataReader fileStatusListProvider,
                                      List<OmBucketInfo> omBucketInfos) throws IOException {
    List<List<OzoneFileStatusLight>> listOfOFSPerIteration = new ArrayList<>(omBucketInfos.size());
    for (OmBucketInfo bucket: omBucketInfos) {
      List<OzoneFileStatusLight> list1 = stubTheNextListOfKeyStatuses(bucket, Arrays.asList(
          Pair.of(".snapshot/snapshot-1/key1", false), Pair.of(".snapshot/snapshot-1/key2", false),
          Pair.of(".snapshot/snapshot-1/dir1", true)));
      List<OzoneFileStatusLight> list2 = stubTheNextListOfKeyStatuses(bucket, Arrays.asList(
          Pair.of(".snapshot/snapshot-1/dir1/key3", false), Pair.of(".snapshot/snapshot-1/dir1/key4", false)));
      listOfOFSPerIteration.add(list1);
      listOfOFSPerIteration.add(list2);
      listOfOFSPerIteration.add(Collections.singletonList(list2.get(list2.size() - 1)));
    }
    OngoingStubbing<List<OzoneFileStatusLight>> keyStatusListStub = when(fileStatusListProvider.listStatusLight(
        any(OmKeyArgs.class), anyBoolean(), anyString(), anyLong(), anyBoolean()));
    for (List<OzoneFileStatusLight> list: listOfOFSPerIteration) {
      keyStatusListStub = keyStatusListStub.thenReturn(list);
    }
  }

  private void mockKeyListPerBucket(List<OmBucketInfo> buckets,
                                                          IOmMetadataReader fileStatusListProvider,
                                                          int keysPerBucket) throws IOException {
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    OngoingStubbing<List<OzoneFileStatusLight>> fileStatusListStub = when(fileStatusListProvider.listStatusLight(
        any(OmKeyArgs.class), anyBoolean(), anyString(), anyLong(), anyBoolean()));
    for (OmBucketInfo bucket: buckets) {
      List<OzoneFileStatusLight> fileStatuses = new ArrayList<>(keysPerBucket);
      for (int i = 0; i < keysPerBucket; i++) {
        BasicOmKeyInfo keyInfo = new BasicOmKeyInfo.Builder()
            .setVolumeName(bucket.getVolumeName())
            .setBucketName(bucket.getName())
            .setKeyName("key" + i)
            .setDataSize(2048)
            .setReplicationConfig(replicationConfig)
            .setIsFile(true)
            .build();
        OzoneFileStatusLight fileStatus = new OzoneFileStatusLight(keyInfo, 2048, false);
        fileStatuses.add(fileStatus);
      }
      fileStatusListStub = fileStatusListStub.thenReturn(fileStatuses)
          .thenReturn(Collections.singletonList(fileStatuses.get(fileStatuses.size() - 1)));
    }
  }

  private List<OmBucketInfo> mockBucketsListPerVolume(BucketManager bucketManager, List<OmVolumeArgs> listOfVolumes,
                                                int bucketsPerVolume)
      throws IOException {
    List<OmBucketInfo> totalListOfBuckets = new ArrayList<>(listOfVolumes.size() * bucketsPerVolume);
    for (OmVolumeArgs volume: listOfVolumes) {
      List<OmBucketInfo> listOfBuckets = new ArrayList<>(bucketsPerVolume);
      for (int i = 1; i <= bucketsPerVolume; i++) {
        listOfBuckets.add(new OmBucketInfo.Builder()
            .setVolumeName(volume.getName())
            .setBucketName("bucket" + i)
            .setCreationTime(System.currentTimeMillis())
            .build());
      }
      totalListOfBuckets.addAll(listOfBuckets);
      when(bucketManager.listBuckets(eq(volume.getName()), eq(null), eq(null), anyInt(), eq(false)))
          .thenReturn(listOfBuckets)
          .thenReturn(Collections.emptyList());
    }
    return totalListOfBuckets;
  }

  private List<OmVolumeArgs> listOfVolumesOfSize(int size) {
    List<OmVolumeArgs> volumes = new ArrayList<>(size);
    for (int i = 1; i <= size; i++) {
      volumes.add(spy(new OmVolumeArgs.Builder()
          .setVolume("vol" + i)
          .setOwnerName("ozone")
          .setAdminName("ozone")
          .build()));
    }
    return volumes;
  }

}
