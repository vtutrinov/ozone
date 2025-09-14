package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.multiraft.OMHAMultiRaftMetrics;
import org.apache.hadoop.ozone.om.ratis.BucketStateMachine;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test MultiRaft.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class TestMultiRaft {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiRaft.class);

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;

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
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omServiceId = "omServiceId1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, isMultiRaftEnabled);
    if (maxMultiRaftGroup != null) {
      conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, maxMultiRaftGroup);
    }

    int numOfOMs = 3;
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
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testRaftGroupsAndStateMachinesWhenMultiRaftDisables()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(false);
    OzoneManager om = cluster.getOMLeader();
    assertEquals(1, om.getOmRaftGroups().size());
    assertEquals(1, om.getStateMachines().size());
  }

  @Test
  void testDefaultRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true);
    OzoneManager om = cluster.getOMLeader();

    waitFor(
        () -> om.getOmRaftGroups().size() == 5,
        100,
        80000
    );
    assertEquals(5, om.getOmRaftGroups().size());
    waitFor(
        () -> om.getStateMachines().size() == 5,
        100,
        80000
    );
    assertEquals(5, om.getStateMachines().size());
  }

  @Test
  void testRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 10);
    OzoneManager om = cluster.getOMLeader();
    waitFor(
        () -> om.getOmRaftGroups().size() == 11
              && om.getStateMachines().size() == 11,
        100,
        80000
    );
    assertEquals(11, om.getOmRaftGroups().size());
    assertEquals(11, om.getStateMachines().size());
  }

  @Test
  void testChangeMultiRaftConfig() throws InterruptedException, TimeoutException, IOException {
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

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 1);

    String key1 = "testkey1";
    writeKey(proxy, volume, bucket, key1);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitFor(() -> om1.getOmRaftGroups().size() == 5 &&
                                   om2.getOmRaftGroups().size() == 5 &&
                                   om3.getOmRaftGroups().size() == 5, 100, 80000);
    assertEquals(5, om1.getOmRaftGroups().size());
    assertEquals(5, om2.getOmRaftGroups().size());
    assertEquals(5, om3.getOmRaftGroups().size());

    String key2 = "testkey2";
    writeKey(proxy, volume, bucket, key2);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 1);

    String key3 = "testkey3";
    writeKey(proxy, volume, bucket, key3);

    checkKeyReading(volume, bucket, key);
    checkKeyReading(volume, bucket, key1);
    checkKeyReading(volume, bucket, key2);
    checkKeyReading(volume, bucket, key3);
  }

  @Test
  void testChangeMultiRaftGroupsSize() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 11);

    ClientProtocol proxy = cluster.createClient().getProxy();
    String volume = "testvolume";
    proxy.createVolume(volume);
    String bucket = "testbucket";
    proxy.createBucket(volume, bucket);
    OzoneBucket bucketDetails = proxy.getBucketDetails(volume, bucket);
    assertNotNull(bucketDetails);
    assertEquals("testbucket", bucketDetails.getName());
    assertEquals("testvolume", bucketDetails.getVolumeName());
    sleep(1000L);

    String key = "testkey";
    writeKey(proxy, volume, bucket, key);
    sleep(1000L);

    OzoneKeyDetails keyDetails = proxy.getKeyDetails(volume, bucket, key);
    assertNotNull(keyDetails);
    assertEquals("testkey", keyDetails.getName());

    checkKeyReading(volume, bucket, key);
  }

  @Test
  void testWriteRequestsExecutesThroughBucketStateMachines()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 1);

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om0, om1, om2, 2);

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

    sleep(1000L);
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

  private void writeKey(ClientProtocol proxy, String volume, String bucket4, String key) throws IOException {
    writeKey(proxy, volume, bucket4, key, key);
  }

  private void writeKey(ClientProtocol proxy, String volume, String bucket, String key, String fileText)
      throws IOException {
    try (OzoneOutputStream stream = proxy.createKey(
        volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(fileText.getBytes(UTF_8));
    }
  }

  @Test
  void testUpdateFileAfterMultiRaftReconfiguration() throws InterruptedException, TimeoutException, IOException {
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

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 1);

    String key1 = "testkey1";
    writeKey(proxy, volume, bucket, key1);
    long keyUpdateId1 = getKeyUpdateId(volume, bucket, key1);
    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 5);
    assertEquals(5, om2.getOmRaftGroups().size());

    writeKey(proxy, volume, bucket, key1, "updated text 1");

    waitFor(
        () -> isKeyEquals(volume, bucket, key1, "updated text 1"),
        500,
        2000
    );
    checkKeyReading(volume, bucket, key1, "updated text 1");

    long keyUpdateId2 = getKeyUpdateId(volume, bucket, key1);
    assertTrue(keyUpdateId2 < keyUpdateId1);
    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 1);

    writeKey(proxy, volume, bucket, key1, "updated text 2");
    checkKeyReading(volume, bucket, key1, "updated text 2");

    long keyUpdateId3 = getKeyUpdateId(volume, bucket, key1);
    assertTrue(keyUpdateId3 > keyUpdateId2);
  }

  @Test
  void testCleaningRatisDirectory() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 5);

    UUID mainGroupUuid = cluster.getOzoneManager(0).getOmRatisServer().getCurrentRaftGroupId().getUuid();
    List<String> dirsListBefore0 = getDirsList(om1.getConfiguration());
    assertTrue(dirsListBefore0.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore1 = getDirsList(om2.getConfiguration());
    assertTrue(dirsListBefore1.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore2 = getDirsList(om3.getConfiguration());
    assertTrue(dirsListBefore2.contains(mainGroupUuid.toString()));

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 5);
    waitMultiRaftTerm(2);
    checkRemovedDirsDirs(om1, mainGroupUuid, dirsListBefore0);
    checkRemovedDirsDirs(om2, mainGroupUuid, dirsListBefore1);
    checkRemovedDirsDirs(om3, mainGroupUuid, dirsListBefore2);
  }

  @Test
  void testGroupsCorrectCreatingWhenLeaderChangingBetweenReconcilerCycles()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 5);

    OzoneManager omLeader = cluster.getOMLeader();

    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, false);

    waitOneOfOmRaftGroupsSizeOnNodesLess(om1, om2, om3, 5);

    waitLeaderElection();

    Stream.of(om1, om2, om3)
        .min(Comparator.comparingInt(el -> el.getOmRaftGroups().size()))
        .ifPresent(it -> {
              try {
                cluster.getOMLeader().transferLeadership(it.getOMNodeId());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
        );

    waitLeaderElection();

    OzoneManager newOmLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(newOmLeader);
    cluster.restartOzoneManager(newOmLeader, false);

    waitLeaderElection();

    waitMultiRaftTerm(2);
    waitOmRaftGroupsSizeOnNodesEqual(cluster.getOzoneManager(0), cluster.getOzoneManager(1), cluster.getOzoneManager(2),5);
    assertAll("Assert groups count",
        () -> assertEquals(5, cluster.getOzoneManager(0).getOmRaftGroups().size()),
        () -> assertEquals(5, cluster.getOzoneManager(1).getOmRaftGroups().size()),
        () -> assertEquals(5, cluster.getOzoneManager(2).getOmRaftGroups().size())
    );
  }

  @Test
  void testMultiRaftTermIncreasing()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om0, om1, om2, 5);

    OzoneManager omLeader = cluster.getOMLeader();

    waitMultiRaftTerm(1);

    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, false);

    waitMultiRaftTerm(1);
    assertEquals(1, cluster.getOMLeader().getCurrentMultiRaftTerm());

    waitLeaderElection();

    OzoneManager newOmLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(newOmLeader);
    cluster.restartOzoneManager(newOmLeader, false);

    waitLeaderElection();

    waitOmRaftGroupsSizeOnNodesEqual(om0, om1, om2, 5);
    waitMultiRaftTerm(2);
    assertEquals(2, cluster.getOMLeader().getCurrentMultiRaftTerm());
  }

  @Test
  void testSetNotNumberConfigParam()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);

    om1.getConfiguration().set(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, "abcd");

    cluster.shutdownOzoneManager(om1);
    NumberFormatException numberFormatException =
        assertThrows(NumberFormatException.class, () -> cluster.restartOzoneManager(om1, false));
    assertEquals("For input string: \"abcd\"", numberFormatException.getMessage());
  }

  @Test
  void testSetNegativeNumberConfigParam()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    om1.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);
    om2.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);
    om3.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 7);
    assertEquals(7, om1.getOmRaftGroups().size());
    assertEquals(7, om2.getOmRaftGroups().size());
    assertEquals(7, om3.getOmRaftGroups().size());
  }

  @Test
  void testMetricAfterGroupCreating()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 5);

    checkNodeStatistic(om1, 5);
    checkNodeStatistic(om2, 5);
    checkNodeStatistic(om3, 5);
  }

  @Test
  void testMetricAfterGroupReconfiguration()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 11);

    checkNodeStatistic(om1, 11);
    checkNodeStatistic(om2, 11);
    checkNodeStatistic(om3, 11);
  }

  @Test
  void testMetricAfterDisableMultiRaft()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    waitLeaderElection();
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, 1);

    checkNodeStatistic(om1, 1);
    checkNodeStatistic(om2, 1);
    checkNodeStatistic(om3, 1);
  }

  private static void checkNodeStatistic(OzoneManager om, int omRaftGroups) throws TimeoutException, InterruptedException {
    OMHAMultiRaftMetrics omMultiRaftMetrics = om.getOmMultiRaftMetrics();
    omMultiRaftMetrics.getOmRaftGroupsCount();
    waitFor(() -> omMultiRaftMetrics.getOmRaftGroupsCount() == omRaftGroups, 100, 100_000);
//    waitFor(() -> omMultiRaftMetrics.getIsOzoneManagerInSafeMode() == 0, 100, 100_000);
//    waitFor(() -> omMultiRaftMetrics.getRaftGroupsExpectedCount() == omRaftGroups, 100, 100_000);
    Assertions.assertEquals(omRaftGroups, omMultiRaftMetrics.getOmRaftGroupsCount());
//    Assertions.assertEquals(omRaftGroups, omMultiRaftMetrics.getRaftGroupsExpectedCount());
//TODO Why after disable multi raft ozone manager in safe mode? Should not be.
//    Assertions.assertEquals(0, omMultiRaftMetrics.getIsOzoneManagerInSafeMode());

    MetricsCollectorImpl omMultiRaftMetricsCollector = new MetricsCollectorImpl();
    omMultiRaftMetrics.getMetrics(omMultiRaftMetricsCollector, true);
    Assertions.assertEquals(3, omMultiRaftMetricsCollector.getRecords().size());

    Map<RaftGroupId, String> omHaInfoRaftGroupsLeaders = om.getOmhaMetrics().getOmHaInfoRaftGroupsLeaders();
    waitFor(() -> om.getOmhaMetrics().getOmHaInfoRaftGroupsLeaders().size() == omRaftGroups, 100, 100_000);
    MetricsCollectorImpl omHaMetricsCollector = new MetricsCollectorImpl();
    om.getOmhaMetrics().getMetrics(omHaMetricsCollector, true);
    assertEquals(omRaftGroups, omHaInfoRaftGroupsLeaders.size());
    Assertions.assertEquals(omRaftGroups + 1, omHaMetricsCollector.getRecords().size());
  }


  private static void waitOmRaftGroupsSizeOnNodesEqual(OzoneManager om0, OzoneManager om1, OzoneManager om2, int expectedGroupSize)
      throws TimeoutException, InterruptedException {
    waitFor(
        () ->
            om0.getOmRaftGroups().size() == expectedGroupSize &&
                om1.getOmRaftGroups().size() == expectedGroupSize &&
                om2.getOmRaftGroups().size() == expectedGroupSize,
        1000,
        120000
    );
  }

  private static void waitOneOfOmRaftGroupsSizeOnNodesLess(OzoneManager om0, OzoneManager om1, OzoneManager om2, int expectedGroupSize)
      throws TimeoutException, InterruptedException {
    waitFor(
        () ->
            om0.getOmRaftGroups().size() < expectedGroupSize ||
                om1.getOmRaftGroups().size() < expectedGroupSize ||
                om2.getOmRaftGroups().size() < expectedGroupSize,
        1000,
        80000
    );
  }

  private static void waitOmRaftGroupsSizeOnNodesEqual(OzoneManager om, int expectedGroupSize)
      throws TimeoutException, InterruptedException {
    waitFor(
        () ->
            om.getOmRaftGroups().size() == expectedGroupSize,
        1000,
        80000
    );
  }

  private void waitMultiRaftTerm(int expectedTerm) throws TimeoutException, InterruptedException {
    waitFor(
        () -> {
          OzoneManager omLeader = cluster.getOMLeader();
          if (omLeader == null) {
            return false;
          } else {
            return omLeader.getCurrentMultiRaftTerm() == expectedTerm;
          }
        },
        1000,
        80000
    );
  }

  private void waitLeaderElection() throws TimeoutException, InterruptedException {
    waitFor(
        () -> cluster.getOMLeader() != null, 100,
        (int) Duration.of(80, ChronoUnit.SECONDS).toMillis()
    );
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

  private void checkKeyReading(String volume, String bucket, String key, String expectedText) throws IOException {
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
      assertEquals(expectedText, result);
    }
  }

  private boolean isKeyEquals(String volume, String bucket, String key, String expectedText){
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
      return expectedText.equals(result);
    } catch (Exception e) {
      LOG.error("Error checking key reading", e);
      return false;
    }
  }

  private long getKeyUpdateId(String volume, String bucket, String key) throws IOException {
    OzoneKeyDetails ozoneKeyDetails = cluster.createClient().getProxy().getKeyDetails(volume, bucket, key);
    return ozoneKeyDetails.getUpdateId();
  }

  private static void waitTermIndex(OzoneManager om0, long expectedIndex)
      throws TimeoutException, InterruptedException {
    waitFor(
        () -> om0.getStateMachines().values().stream().filter(BucketStateMachine.class::isInstance)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L) == expectedIndex, 100, 120000);
  }

  private static void checkLastAppliedIndex(Long expectedIndex, OzoneManager om0) {
    Long lastAppliedTermIndex =
        om0.getStateMachines().values().stream().filter(BucketStateMachine.class::isInstance)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L);
    assertEquals(expectedIndex, lastAppliedTermIndex);
  }
}
