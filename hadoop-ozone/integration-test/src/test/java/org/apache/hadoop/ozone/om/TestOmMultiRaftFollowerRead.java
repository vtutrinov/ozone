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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Follower read together with multi-raft: an OM serves a read of the keys of a bucket after the ReadIndex of the
 * bucket raft group (and of the OM raft group), so the reads on followers observe the writes of the bucket raft group.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class TestOmMultiRaftFollowerRead {

  private static final int NUM_OF_OMS = 3;
  private static final int MULTI_RAFT_BUCKET_GROUPS = 2;

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, MULTI_RAFT_BUCKET_GROUPS);
    // OM-to-OM communication of the bucket raft group assignment
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
    OzoneManagerRatisServerConfig omHAConfig = conf.getObject(OzoneManagerRatisServerConfig.class);
    omHAConfig.setReadOption("LINEARIZABLE");
    conf.setFromObject(omHAConfig);
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-follower-read")
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient("om-service-follower-read", conf);
    objectStore = client.getObjectStore();
    waitFor(() -> {
      for (int i = 0; i < NUM_OF_OMS; i++) {
        if (cluster.getOzoneManager(i).getOmRaftGroups().size() != 1 + MULTI_RAFT_BUCKET_GROUPS) {
          return false;
        }
      }
      return true;
    }, 1000, 120_000);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testReadYourWritesWithFollowerRead() throws Exception {
    objectStore.createVolume("vol");
    objectStore.getVolume("vol").createBucket("bucket");
    final OzoneBucket bucket = objectStore.getVolume("vol").getBucket("bucket");
    for (int i = 0; i < 20; i++) {
      final String keyName = "key-" + i;
      writeKey(bucket, keyName);
      assertEquals(keyName.length(), bucket.getKey(keyName).getDataSize());
    }

    // every OM, the followers of the bucket raft group included, serves a linearizable read of its keys
    final OzoneManager leader = cluster.getOMLeader();
    final RaftGroupId group = RaftGroupId.valueOf(leader.getOmRaftGroupManager().getBucketRaftGroups()
        .get(leader.getMetadataManager().getBucketKey("vol", "bucket")));
    final RaftPeerId groupLeader = leader.getOmRatisServer().getLeaderId(group);
    int followers = 0;
    for (int i = 0; i < NUM_OF_OMS; i++) {
      final OzoneManager om = cluster.getOzoneManager(i);
      if (!om.getOmRatisServer().getRaftPeerId().equals(groupLeader)) {
        followers++;
      }
      assertEquals(Status.OK, submitAsRpcCall(om, lookupKey("key-19")).getStatus(), om.getOMNodeId());
    }
    assertEquals(NUM_OF_OMS - 1, followers);
  }

  /** Submits the request to the OM like an RPC call: Ratis requests take the client and call ids of the call. */
  private static OMResponse submitAsRpcCall(OzoneManager om, OMRequest request) throws ServiceException {
    RPC.Server.getCurCall().set(new Server.Call((int) OzoneManagerRatisServer.nextCallId(), 0, null, null,
        RPC.RpcKind.RPC_BUILTIN, ClientId.randomId().toByteString().toByteArray()));
    try {
      return om.getOmServerProtocol().submitRequest(null, request);
    } finally {
      RPC.Server.getCurCall().remove();
    }
  }

  private static OMRequest lookupKey(String keyName) {
    return OMRequest.newBuilder()
        .setCmdType(Type.LookupKey)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(UUID.randomUUID().toString())
        .setReadConsistencyHint(ReadConsistencyHint.newBuilder()
            .setReadConsistency(ReadConsistencyProto.LINEARIZABLE_ALLOW_FOLLOWER))
        .setLookupKeyRequest(LookupKeyRequest.newBuilder().setKeyArgs(KeyArgs.newBuilder()
            .setVolumeName("vol").setBucketName("bucket").setKeyName(keyName)))
        .build();
  }

  private static void writeKey(OzoneBucket bucket, String keyName) throws IOException {
    try (OzoneOutputStream stream = bucket.createKey(keyName, keyName.length(), ReplicationConfig.getDefault(conf),
        Collections.emptyMap())) {
      stream.write(keyName.getBytes(UTF_8));
    }
  }
}
