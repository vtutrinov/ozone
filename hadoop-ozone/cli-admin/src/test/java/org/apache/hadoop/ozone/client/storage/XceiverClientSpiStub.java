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

package org.apache.hadoop.ozone.client.storage;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * XceiverClientSpi implementation with in-memory state.
 */
public class XceiverClientSpiStub extends XceiverClientSpi {
  private final Map<String, Boolean> corruptedBlocks = new HashMap<>();
  private final Pipeline pipeline;

  public XceiverClientSpiStub() {
    DatanodeDetails dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
    this.pipeline = Pipeline.newBuilder()
            .setId(PipelineID.randomId())
            .setState(Pipeline.PipelineState.OPEN)
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setNodes(Collections.singletonList(dn))
            .build();
  }

  @Override
  public void connect() throws Exception {
  }

  @Override
  public void close() {
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return null;
  }

  @Override
  public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) throws
          IOException, ExecutionException, InterruptedException {
    return null;
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return 0;
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
          ContainerCommandRequestProto request) throws IOException, InterruptedException {

    Map<DatanodeDetails, ContainerCommandResponseProto> result = new HashMap<>();
    DatanodeDetails dn = pipeline.getFirstNode();

    if (request.hasVerifyBlock()) {
      VerifyBlockResponseProto verifyBlock = VerifyBlockResponseProto.newBuilder()
              .setValid(true)
              .build();

      ContainerCommandResponseProto response = ContainerCommandResponseProto.newBuilder()
              .setCmdType(Type.VerifyBlock)
              .setResult(Result.SUCCESS)
              .setVerifyBlock(verifyBlock)
              .build();

      result.put(dn, response);
    } else {
      ContainerCommandResponseProto response = ContainerCommandResponseProto.newBuilder()
              .setCmdType(Type.ReadContainer)
              .setResult(Result.SUCCESS)
              .build();
      result.put(dn, response);
    }

    return result;
  }
}
