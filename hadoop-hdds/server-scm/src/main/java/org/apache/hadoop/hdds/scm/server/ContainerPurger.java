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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.ozone.common.BlockGroup.SIZE_NOT_AVAILABLE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SDP (SDPOZN-1910): deletes all the data blocks of a container. The blocks are listed from a replica of the
 * container and handed to the SCM deleted block log, so the datanodes delete them; the then empty container is
 * removed by ReplicationManager. OM metadata is not changed: keys referring to these blocks become unreadable.
 */
public class ContainerPurger {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerPurger.class);

  private static final int LIST_BLOCK_BATCH = 1000;
  private static final int BLOCK_GROUP_SIZE = 10;

  private final StorageContainerManager scm;

  public ContainerPurger(StorageContainerManager scm) {
    this.scm = scm;
  }

  public void purgeContainerWithDataBlocks(ContainerID containerID, Token<? extends TokenIdentifier> token)
      throws IOException {
    // fails with ContainerNotFoundException for an unknown container
    scm.getContainerManager().getContainer(containerID);
    Pipeline pipeline = scm.getPipelineManager().createPipelineForRead(
        StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        scm.getContainerManager().getContainerReplicas(containerID));

    List<BlockGroup> blockGroups = new ArrayList<>();
    try (XceiverClientManager clientManager = newClientManager()) {
      XceiverClientSpi client = clientManager.acquireClient(pipeline);
      try {
        List<DeletedBlock> group = new ArrayList<>();
        Long startLocalID = null;
        while (true) {
          List<ContainerProtos.BlockData> blocks = ContainerProtocolCalls.listBlock(client,
              containerID.getId(), startLocalID, LIST_BLOCK_BATCH, token).getBlockDataList();
          for (ContainerProtos.BlockData blockData : blocks) {
            group.add(new DeletedBlock(new BlockID(blockData.getBlockID().getContainerID(),
                blockData.getBlockID().getLocalID()), SIZE_NOT_AVAILABLE, SIZE_NOT_AVAILABLE, SIZE_NOT_AVAILABLE));
            if (group.size() == BLOCK_GROUP_SIZE) {
              blockGroups.add(newBlockGroup(containerID, blockGroups.size(), group));
              group = new ArrayList<>();
            }
          }
          if (blocks.size() < LIST_BLOCK_BATCH) {
            break;
          }
          startLocalID = blocks.get(blocks.size() - 1).getBlockID().getLocalID() + 1;
        }
        if (!group.isEmpty()) {
          blockGroups.add(newBlockGroup(containerID, blockGroups.size(), group));
        }
      } finally {
        clientManager.releaseClient(client, false);
      }
    }
    LOG.info("Purging container {}: deleting {} block groups", containerID, blockGroups.size());
    scm.getScmBlockManager().deleteBlocks(blockGroups);
  }

  private static BlockGroup newBlockGroup(ContainerID containerID, int groupNumber, List<DeletedBlock> blocks) {
    return BlockGroup.newBuilder()
        .setKeyName(UUID.nameUUIDFromBytes((groupNumber + containerID.toString())
            .getBytes(StandardCharsets.UTF_8)).toString())
        .addAllDeletedBlocks(blocks)
        .build();
  }

  private XceiverClientManager newClientManager() throws IOException {
    if (OzoneSecurityUtil.isSecurityEnabled(scm.getConfiguration())) {
      CertificateClient certClient = scm.getScmCertificateClient();
      return new XceiverClientManager(scm.getConfiguration(),
          scm.getConfiguration().getObject(XceiverClientManager.ScmClientConfig.class),
          certClient.createClientTrustManager());
    }
    return new XceiverClientManager(scm.getConfiguration());
  }
}
