package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the cache used by {@link OmTransportFactory}.
 */
@Timeout(240)
public class OmTransportCacheTest {
  private static MiniOzoneHAClusterImpl cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static OzoneClient client;
  private static String omServiceId;
  private static String scmServiceId;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test";
    scmServiceId = "scm-service-test";
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 2);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setSCMServiceId(scmServiceId)
        .setOMServiceId(omServiceId)
        .setScmId(scmId)
        .setNumDatanodes(3)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(3);
    cluster = (MiniOzoneHAClusterImpl) builder.build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBucketTxId() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    ObjectStore objectStore = client.getObjectStore();

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneManagerProtocolClientSideTranslatorPB omClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, omServiceId),
            RandomStringUtils.randomAscii(5), conf, () -> {
          try {
            return OmTransportFactory.create(conf, ugi, omServiceId);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

    objectStore.createVolume(volumeName);

    // The volume transaction index should be 1 as this is
    // the first transaction in this cluster.
    OmVolumeArgs volumeInfo = omClient.getVolumeInfo(volumeName);
    long volumeTrxnIndex = OmUtils.getTxIdFromObjectId(
        volumeInfo.getObjectID());
    assertEquals(1, volumeTrxnIndex);

    String bucketName1 = "bucket1";
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    ozoneVolume.createBucket(bucketName1);

    String bucketName2 = "bucket2";
    ozoneVolume.createBucket(bucketName2);

    String bucketName3 = "bucket3";
    ozoneVolume.createBucket(bucketName3);

    // Verify last transactionIndex is updated after bucket creation
    OmBucketInfo bucketInfo = omClient.getBucketInfo(volumeName, bucketName1);
    long bucketTrxnIndex = OmUtils.getTxIdFromObjectId(
        bucketInfo.getObjectID());
    assertEquals(3, bucketTrxnIndex);

    String data = "random data";
    OzoneOutputStream ozoneOutputStream1 = ozoneVolume.getBucket(bucketName1)
        .createKey(keyName, data.length(), ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream1.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream1.close();

    OzoneOutputStream ozoneOutputStream2 = ozoneVolume.getBucket(bucketName2)
        .createKey(keyName, data.length(), ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream2.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream2.close();

    OzoneOutputStream ozoneOutputStream3 = ozoneVolume.getBucket(bucketName3)
        .createKey(keyName, data.length(), ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream3.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream3.close();

    // Verify last transactionIndex is 1 as we are in a separate statemachine.
    OmKeyInfo omKeyInfo1 = omClient.lookupKey(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName1)
        .setKeyName(keyName)
        .build());
    long keyTrxnIndex1 = OmUtils.getTxIdFromObjectId(
        omKeyInfo1.getObjectID());
    assertEquals(1, keyTrxnIndex1);

    // Verify last transactionIndex is 1 as we are in a separate statemachine.
    OmKeyInfo omKeyInfo2 = omClient.lookupKey(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName1)
        .setKeyName(keyName)
        .build());
    long keyTrxnIndex2 = OmUtils.getTxIdFromObjectId(
        omKeyInfo2.getObjectID());
    assertEquals(1, keyTrxnIndex2);

    // Verify last transactionIndex is 1 as we are in a separate statemachine.
    OmKeyInfo omKeyInfo3 = omClient.lookupKey(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName1)
        .setKeyName(keyName)
        .build());
    long keyTrxnIndex3 = OmUtils.getTxIdFromObjectId(
        omKeyInfo3.getObjectID());
    assertEquals(1, keyTrxnIndex3);
  }
}
