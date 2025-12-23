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

package org.apache.hadoop.ozone.ratelimiters;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.RateLimiterInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.toDBKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * HA integration tests for rate limiters.
 */
public class TestRateLimitersHA {

  private static final String VOLUME = "vol1";
  private static final String BUCKET1 = "bucket1";
  private static final String BUCKET2 = "bucket2";

  private MiniOzoneHAClusterImpl cluster;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
  private OzoneClient client;
  private RpcClient rpcClient;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "omServiceId1";

    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
            OZONE_ADMINISTRATORS_WILDCARD);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
            .setClusterId(clusterId)
            .setScmId(scmId)
            .setOMServiceId(omServiceId)
            .setNumOfOzoneManagers(numOfOMs)
            .build();

    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    rpcClient = (RpcClient) client.getObjectStore().getClientProxy();

    client.getObjectStore().createVolume(VOLUME);
    OzoneVolume volume = client.getObjectStore().getVolume(VOLUME);
    volume.createBucket(BUCKET1);
    volume.createBucket(BUCKET2);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRateLimiterConfigReplicatedToAllOzoneManagers()
          throws Exception {

    int rps = 1;
    RateLimiterType type = RateLimiterType.WRITE;

    rpcClient.createRateLimiter(VOLUME, BUCKET1, rps, type);

    waitFor(() -> {
      try {
        return allOmHaveRateLimiter(VOLUME, BUCKET1, rps, type);
      } catch (IOException e) {
        return false;
      }
    }, 500, 10_000);
  }

  @Test
  public void testWriteRequestsAreRateLimitedPerBucket() throws Exception {
    int rps = 10;
    RateLimiterType type = RateLimiterType.WRITE;

    rpcClient.createRateLimiter(VOLUME, BUCKET2, rps, type);

    OzoneVolume volume = client.getObjectStore().getVolume(VOLUME);
    OzoneBucket bucket2 = volume.getBucket(BUCKET2);

    boolean rejected = false;
    for (int i = 0; i < 100; i++) {
      try {
        bucket2.createKey("k-" + i, 1);
      } catch (IOException e) {
        String msg = e.getMessage();
        if (msg != null && msg.contains("Rate limit exceeded")) {
          rejected = true;
          break;
        } else {
          throw e;
        }
      }
    }
    assertTrue("Write requests on bucket2 must be limited", rejected);
  }

  @Test
  public void testDuplicateCreateDoesNotOverride()
          throws Exception {

    int initialRps = 10;
    RateLimiterType type = RateLimiterType.WRITE;

    rpcClient.createRateLimiter(VOLUME, BUCKET1, initialRps, type);

    waitFor(() -> {
      try {
        return allOmHaveRateLimiter(VOLUME, BUCKET1, initialRps, type);
      } catch (IOException e) {
        return false;
      }
    }, 200, 10_000);

    int newRps = 200;

    try {
      rpcClient.createRateLimiter(VOLUME, BUCKET1, newRps, type);
      fail("Expected OMException with RATELIMITER_ALREADY_EXISTS");
    } catch (OMException e) {
      assertEquals("Unexpected result code",
              OMException.ResultCodes.RATELIMITER_ALREADY_EXISTS, e.getResult());
    }

    assertTrue(
            "Existing rate limiter config must remain unchanged after duplicate create",
            allOmHaveRateLimiter(VOLUME, BUCKET1, initialRps, type));
  }

  private boolean allOmHaveRateLimiter(String volume, String bucket,
                                       int expectedRps,
                                       RateLimiterType type) throws IOException {
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      OMMetadataManager metadataManager = om.getMetadataManager();

      String dbKey = toDBKey(metadataManager, volume, bucket, type);

      RateLimiterInfo info = metadataManager.getRateLimiterInfoTable().get(dbKey);
      if (info == null) {
        return false;
      }
      if (info.getRps() != expectedRps
                  || info.getType() != type
                  || !volume.equals(info.getVolumeName())
                  || !bucket.equals(info.getBucketName())) {
        return false;
      }
    }
    return true;
  }
}

