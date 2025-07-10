package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test MultiRaft.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class TestMultiRaft {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;

  /**
   * Create a MiniOzoneHAClusterImpl for testing.
   *
   * @throws IOException
   */
  private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled)
          throws IOException, InterruptedException, TimeoutException {
    return initClusterWithMultiRaft(isMultiRaftEnabled, null);
  }

  private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled, Integer maxMultiRaftGroup)
          throws IOException, TimeoutException, InterruptedException {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "omServiceId1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
            OZONE_ADMINISTRATORS_WILDCARD);
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, isMultiRaftEnabled);
    if (maxMultiRaftGroup != null) {
      conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, maxMultiRaftGroup);
    }

    MiniOzoneHAClusterImpl currentCluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
            .setClusterId(clusterId)
            .setScmId(scmId)
            .setOMServiceId(omServiceId)
            .setNumOfOzoneManagers(numOfOMs)
            .build();
    currentCluster.waitForClusterToBeReady();
    return currentCluster;
  }

  /**
   * Shutdown MiniOzoneHAClusterImpl.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRaftGroupsAndStateMachinesWhenMultiRaftDisables()
          throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(false);
    OzoneManager om = cluster.getOMLeader();
    assertEquals(1, om.getOmRaftGroups().size());
    assertEquals(1, om.getStateMachines().size());
  }

  @Test
  public void testDefaultRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
          throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true);
    OzoneManager om = cluster.getOMLeader();

    GenericTestUtils.waitFor(() -> om.getOmRaftGroups().size() == 5, 100, 20000
    );
    assertEquals(5, om.getOmRaftGroups().size());
    assertEquals(5, om.getStateMachines().size());
  }

  @Test
  public void testRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
          throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 10);
    OzoneManager om = cluster.getOMLeader();
    assertEquals(11, om.getOmRaftGroups().size());
    assertEquals(11, om.getStateMachines().size());
  }

  @Test
  public void testChangeMultiRaftConfig() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    ClientProtocol proxy = cluster.createClient().getProxy();
    String volume = "testvolume";
    proxy.createVolume(volume);
    String bucket = "testbucket";
    proxy.createBucket(volume, bucket);
    String key = "testkey";
    try (OzoneOutputStream stream = proxy.createKey(
            volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap()
    )
    ) {
      stream.write(key.getBytes(UTF_8));
    }

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);
    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
            om1.getOmRaftGroups().size() == 1 &&
            om2.getOmRaftGroups().size() == 1, 100, 20000);

    assertEquals(1, om0.getOmRaftGroups().size());
    assertEquals(1, om1.getOmRaftGroups().size());
    assertEquals(1, om2.getOmRaftGroups().size());

    String key1 = "testkey1";
    try (OzoneOutputStream stream = proxy
            .createKey(volume, bucket, key1, key1.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(key1.getBytes(UTF_8));
    }

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 5 &&
            om1.getOmRaftGroups().size() == 5 &&
            om2.getOmRaftGroups().size() == 5, 100, 20000);
    assertEquals(5, om1.getOmRaftGroups().size());

    String key2 = "testkey2";
    try (OzoneOutputStream stream = proxy
            .createKey(volume, bucket, key2, key2.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(key2.getBytes(UTF_8));
    }

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
            om1.getOmRaftGroups().size() == 1 &&
            om2.getOmRaftGroups().size() == 1, 100, 20000);
    assertEquals(1, om1.getOmRaftGroups().size());

    String key3 = "testkey3";
    try (OzoneOutputStream stream = proxy
            .createKey(volume, bucket, key3, key3.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(key3.getBytes(UTF_8));
    }

    checkKeyReading(volume, bucket, key);
    checkKeyReading(volume, bucket, key1);
    checkKeyReading(volume, bucket, key2);
    checkKeyReading(volume, bucket, key3);
  }


  @Test
  public void testChangeMultiRaftGroupsSize() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 11 &&
            om1.getOmRaftGroups().size() == 11 &&
            om2.getOmRaftGroups().size() == 11, 100, 120000);
    assertEquals(11, om1.getOmRaftGroups().size());

    ClientProtocol proxy = cluster.createClient().getProxy();
    String volume = "testvolume";
    proxy.createVolume(volume);
    String bucket = "testbucket";
    proxy.createBucket(volume, bucket);
    OzoneBucket bucketDetails = proxy.getBucketDetails(volume, bucket);
    assertNotNull(bucketDetails);
    assertEquals("testbucket", bucketDetails.getName());
    assertEquals("testvolume", bucketDetails.getVolumeName());
    String key = "testkey";
    try (OzoneOutputStream stream = proxy.createKey(
            volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(key.getBytes(UTF_8));
    }

    OzoneKeyDetails keyDetails = proxy.getKeyDetails(volume, bucket, key);
    assertNotNull(keyDetails);
    assertEquals("testkey", keyDetails.getName());

    checkKeyReading(volume, bucket, key);
  }

  private void checkKeyReading(String volume, String bucket, String key) throws IOException {
    try (
            OzoneInputStream ozoneInputStream = cluster.createClient().getProxy().getKey(volume, bucket, key);
            BufferedReader br = new BufferedReader(new InputStreamReader(ozoneInputStream, UTF_8));
    ) {

      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      String result = sb.toString();
      assertEquals(key, result);
    }
  }
}
