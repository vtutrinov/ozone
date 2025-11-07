package org.apache.hadoop.ozone.om.request.bucket;


import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addKeyToOM;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createOmKeyInfo;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createRefreshBucketUsedBytesRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests OMRefreshBucketUsedBytesRequest class, which handles RefreshBucketUsedBytes request.
 */
public class TestOMRefreshBucketUsedBytesRequest extends TestBucketRequest {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";

  @Test
  public void testUsedBytesRecalculation() throws Exception {
    long expectedUsedBytes = 100 + 800 + 50;
    OmKeyInfo key1 = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key1",
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            1, 1, System.currentTimeMillis(), 0);

    OmKeyInfo key2 = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key2",
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            2, 2, System.currentTimeMillis(), 0);

    OmKeyInfo key3 = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key3",
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            3, 3, System.currentTimeMillis(), 0);

    key1.setDataSize(100);
    key2.setDataSize(800);
    key3.setDataSize(50);

    addVolumeAndBucketToDB(VOLUME_NAME, BUCKET_NAME, omMetadataManager);
    addKeyToOM(omMetadataManager, key1);
    addKeyToOM(omMetadataManager, key2);
    addKeyToOM(omMetadataManager, key3);

    OMRefreshBucketUsedBytesRequest refreshRequest = doPreExecute(VOLUME_NAME, BUCKET_NAME);
    OmBucketInfo bucketInfo = getBucketInfo(expectedUsedBytes);

    when(ozoneManager.getBucketInfo(VOLUME_NAME, BUCKET_NAME)).thenReturn(bucketInfo);

    OMClientResponse response = refreshRequest.validateAndUpdateCache(ozoneManager, 1);

    long actual = response.getOMResponse().getRefreshBucketUsedBytesResponse().getUsedBytes();

    assertEquals(expectedUsedBytes, actual, "usedBytes must be equal to the sum of all dataSize");
    assertTrue(response.getOMResponse().getSuccess());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, 0})
  public void testEmptyBucketWithNullOrNegativeUsedBytes(int usedBytes) throws Exception {
    OMRefreshBucketUsedBytesRequest refreshRequest = doPreExecute(VOLUME_NAME, BUCKET_NAME);
    addVolumeAndBucketToDB(VOLUME_NAME, BUCKET_NAME, omMetadataManager);
    OmBucketInfo bucketInfo = getBucketInfo(usedBytes);

    when(ozoneManager.getBucketInfo(VOLUME_NAME, BUCKET_NAME)).thenReturn(bucketInfo);

    OMClientResponse response = refreshRequest.validateAndUpdateCache(ozoneManager, 1);

    long actual = response.getOMResponse().getRefreshBucketUsedBytesResponse().getUsedBytes();

    assertEquals(0, actual);
    assertTrue(response.getOMResponse().getSuccess());
  }

  @Test
  public void testBucketWithNegativeUsedBytesAndPositiveDataSize() throws Exception {
    OMRefreshBucketUsedBytesRequest refreshRequest = doPreExecute(VOLUME_NAME, BUCKET_NAME);
    OmKeyInfo key1 = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key1",
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            3, 3, System.currentTimeMillis(), 0);
    key1.setDataSize(100);

    addVolumeAndBucketToDB(VOLUME_NAME, BUCKET_NAME, omMetadataManager);
    addKeyToOM(omMetadataManager, key1);
    OmBucketInfo bucketInfo = getBucketInfo(-1);

    when(ozoneManager.getBucketInfo(VOLUME_NAME, BUCKET_NAME)).thenReturn(bucketInfo);

    OMClientResponse response = refreshRequest.validateAndUpdateCache(ozoneManager, 1);

    long actual = response.getOMResponse().getRefreshBucketUsedBytesResponse().getUsedBytes();

    assertEquals(100, actual);
    assertTrue(response.getOMResponse().getSuccess());
  }

  @Test
  public void testRecalculationWithManyKeys() throws Exception {
    int expectedUsedBytes = 15000;
    addVolumeAndBucketToDB(VOLUME_NAME, BUCKET_NAME, omMetadataManager);
    for (int i = 0; i < 1500; i++) {
      OmKeyInfo key = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key" + i,
                HddsProtos.ReplicationType.RATIS,
                HddsProtos.ReplicationFactor.THREE,
                i, i, System.currentTimeMillis(), 0);
      key.setDataSize(10);
      addKeyToOM(omMetadataManager, key);
    }
    OmBucketInfo bucketInfo = getBucketInfo(expectedUsedBytes);
    OMRefreshBucketUsedBytesRequest refreshRequest = doPreExecute(VOLUME_NAME, BUCKET_NAME);

    when(ozoneManager.getBucketInfo(VOLUME_NAME, BUCKET_NAME)).thenReturn(bucketInfo);

    OMClientResponse response = refreshRequest.validateAndUpdateCache(ozoneManager, 1);

    long actual = response.getOMResponse().getRefreshBucketUsedBytesResponse().getUsedBytes();

    assertEquals(expectedUsedBytes, actual);
    assertTrue(response.getOMResponse().getSuccess());
  }

  @Test
  public void testZeroDataSize() throws Exception {
    long expectedUsedBytes = 0;
    OmKeyInfo key1 = createOmKeyInfo(VOLUME_NAME, BUCKET_NAME, "key1",
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            1, 1, System.currentTimeMillis(), 0);
    key1.setDataSize(0);
    addVolumeAndBucketToDB(VOLUME_NAME, BUCKET_NAME, omMetadataManager);
    addKeyToOM(omMetadataManager, key1);
    OmBucketInfo bucketInfo = getBucketInfo(expectedUsedBytes);

    OMRefreshBucketUsedBytesRequest refreshRequest = doPreExecute(VOLUME_NAME, BUCKET_NAME);

    when(ozoneManager.getBucketInfo(VOLUME_NAME, BUCKET_NAME)).thenReturn(bucketInfo);

    OMClientResponse response = refreshRequest.validateAndUpdateCache(ozoneManager, 1);

    long actual = response.getOMResponse().getRefreshBucketUsedBytesResponse().getUsedBytes();

    assertEquals(expectedUsedBytes, actual, "usedBytes must be equal to the sum of all dataSize");
    assertTrue(response.getOMResponse().getSuccess());
  }

  protected OMRefreshBucketUsedBytesRequest doPreExecute(String volumeName, String bucketName) throws Exception {
    OzoneManagerProtocolProtos.OMRequest originalRequest = createRefreshBucketUsedBytesRequest(volumeName, bucketName);
    OMRefreshBucketUsedBytesRequest refreshUsedBytesRequest = new OMRefreshBucketUsedBytesRequest(originalRequest);

    OzoneManagerProtocolProtos.OMRequest modifiedRequest = refreshUsedBytesRequest.preExecute(ozoneManager);

    verifyRequest(modifiedRequest, originalRequest);
    return new OMRefreshBucketUsedBytesRequest(modifiedRequest);
  }

  protected void verifyRequest(OzoneManagerProtocolProtos.OMRequest modifiedOmRequest,
                             OzoneManagerProtocolProtos.OMRequest originalRequest) {
    OzoneManagerProtocolProtos.BucketInfo original = originalRequest.getCreateBucketRequest().getBucketInfo();
    OzoneManagerProtocolProtos.BucketInfo updated = modifiedOmRequest.getCreateBucketRequest().getBucketInfo();

    assertEquals(original.getBucketName(), updated.getBucketName());
    assertEquals(original.getVolumeName(), updated.getVolumeName());
    assertEquals(original.getIsVersionEnabled(), updated.getIsVersionEnabled());
    assertEquals(original.getStorageType(), updated.getStorageType());
    assertEquals(original.getMetadataList(), updated.getMetadataList());
  }

  private static OmBucketInfo getBucketInfo(long expectedUsedBytes) {
    return OmBucketInfo.newBuilder()
            .setVolumeName(VOLUME_NAME)
            .setBucketName(BUCKET_NAME)
            .setUsedBytes(expectedUsedBytes).build();
  }
}
