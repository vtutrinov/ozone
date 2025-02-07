package org.apache.hadoop.ozone.client.storage;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

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
  public XceiverClientReply watchForCommit(long index)
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
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
