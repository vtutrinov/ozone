package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.BucketStateMachine;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    GenericTestUtils.waitFor(
        () -> om.getOmRaftGroups().size() == 5,
        100,
        80000
    );
    GenericTestUtils.waitFor(
        () -> om.getStateMachines().size() == 5,
        100,
        80000
    );
  }

  @Test
  public void testRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
          throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 10);
    OzoneManager om = cluster.getOMLeader();
    GenericTestUtils.waitFor(
        () -> om.getOmRaftGroups().size() == 11
            && om.getStateMachines().size() == 11,
        100,
        80000
    );
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
    writeKey(proxy, volume, bucket, key);

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
            om2.getOmRaftGroups().size() == 1, 100, 100000);

    String key1 = "testkey1";
    writeKey(proxy, volume, bucket, key1);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 5 &&
            om1.getOmRaftGroups().size() == 5 &&
            om2.getOmRaftGroups().size() == 5, 100, 80000);
    assertEquals(5, om1.getOmRaftGroups().size());

    String key2 = "testkey2";
    writeKey(proxy, volume, bucket, key2);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
            om1.getOmRaftGroups().size() == 1 &&
            om2.getOmRaftGroups().size() == 1, 100, 80000);

    String key3 = "testkey3";
    writeKey(proxy, volume, bucket, key3);

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

    ClientProtocol proxy = cluster.createClient().getProxy();
    String volume = "testvolume";
    proxy.createVolume(volume);
    String bucket = "testbucket";
    proxy.createBucket(volume, bucket);
    OzoneBucket bucketDetails = proxy.getBucketDetails(volume, bucket);
    assertNotNull(bucketDetails);
    assertEquals("testbucket", bucketDetails.getName());
    assertEquals("testvolume", bucketDetails.getVolumeName());
    Thread.sleep(1000L);

    String key = "testkey";
    writeKey(proxy, volume, bucket, key);
    Thread.sleep(1000L);

    OzoneKeyDetails keyDetails = proxy.getKeyDetails(volume, bucket, key);
    assertNotNull(keyDetails);
    assertEquals("testkey", keyDetails.getName());

    checkKeyReading(volume, bucket, key);
  }

  @Test
  public void testWriteRequestsExecutesThroughBucketStateMachines() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 1);

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 2 &&
        om1.getOmRaftGroups().size() == 2 &&
        om2.getOmRaftGroups().size() == 2, 100, 120000);

    ClientProtocol proxy = cluster.createClient().getProxy();
    String volume = "testvolume";
    proxy.createVolume(volume);
    String bucket1 = "111a";
    proxy.createBucket(volume, bucket1);
    String bucket2 = "222a";
    proxy.createBucket(volume, bucket2);
    String bucket3 = "333a";
    proxy.createBucket(volume, bucket3);
    String bucket4 = "444a";
    proxy.createBucket(volume, bucket4);

    String key = "testkey";

    Thread.sleep(1000L);
    writeKey(proxy, volume, bucket1, key);
    waitTermIndex(om0, 4L);
    checkLastAppliedIndex(4L, om0);

    writeKey(proxy, volume, bucket2, key);
    waitTermIndex(om0, 8L);
    checkLastAppliedIndex(8L, om0);

    writeKey(proxy, volume, bucket3, key);
    waitTermIndex(om0, 12L);
    checkLastAppliedIndex(12L, om0);

    writeKey(proxy, volume, bucket4, key);
    waitTermIndex(om0, 16L);
    checkLastAppliedIndex(16L, om0);

    checkKeyReading(volume, bucket1, key);
    checkKeyReading(volume, bucket2, key);
    checkKeyReading(volume, bucket3, key);
    checkKeyReading(volume, bucket4, key);
  }

  private static void waitTermIndex(OzoneManager om0, long expectedIndex) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> om0.getStateMachines().values().stream().filter(it -> it instanceof BucketStateMachine)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L) == expectedIndex, 100, 120000);
  }

  private static void checkLastAppliedIndex(Long expectedIndex,OzoneManager om0) {
    Long lastAppliedTermIndex =
        om0.getStateMachines().values().stream().filter(it -> it instanceof BucketStateMachine)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L);
    assertEquals(expectedIndex, lastAppliedTermIndex);
  }

  private void writeKey(ClientProtocol proxy, String volume, String bucket4, String key) throws IOException {
    try (OzoneOutputStream stream = proxy.createKey(
        volume, bucket4, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(key.getBytes(UTF_8));
    }
  }

  @Test
  public void testCleaningRatisDirectory() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 5 &&
        om1.getOmRaftGroups().size() == 5 &&
        om2.getOmRaftGroups().size() == 5, 1000, 120000);

    UUID mainGroupUuid = cluster.getOzoneManager(0).getOmRatisServer().getCurrentRaftGroupId().getUuid();
    List<String> dirsListBefore0 = getDirsList(om0.getConfiguration());
    assertTrue(dirsListBefore0.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore1 = getDirsList(om1.getConfiguration());
    assertTrue(dirsListBefore1.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore2 = getDirsList(om2.getConfiguration());
    assertTrue(dirsListBefore2.contains(mainGroupUuid.toString()));

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(
        () -> getDirsList(om0.getConfiguration()).size() == 5 ,
        1000,
        120000
    );

    checkRemovedDirsDirs(om0, mainGroupUuid, dirsListBefore0);
    checkRemovedDirsDirs(om1, mainGroupUuid, dirsListBefore1);
    checkRemovedDirsDirs(om2, mainGroupUuid, dirsListBefore2);
  }

  private void checkRemovedDirsDirs(OzoneManager om0, UUID mainGroupUuid, List<String> dirsListBefore0) {
    List<String> dirsListAfter0 = getDirsList(om0.getConfiguration());
    assertTrue(dirsListAfter0.contains(mainGroupUuid.toString()));
    dirsListAfter0.removeAll(dirsListBefore0);
    assertEquals(4, dirsListAfter0.size());
  }

  private List<String> getDirsList(OzoneConfiguration configuration) {
    String omRatisDirectory = ServerUtils.getDefaultRatisDirectory(configuration);
    File ratisMetadataDir = new File(omRatisDirectory);
    if (ratisMetadataDir.exists()) {
      String[] list = ratisMetadataDir.list();

      return new ArrayList<>(Arrays.asList(list == null ? new String[]{} : list));
    }
    return Lists.emptyList();
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
