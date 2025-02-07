package org.apache.hadoop.ozone.client.storage;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * ContainerOperationClient implementation with in-memory state.
 */
public class ContainerOperationClientStub extends ContainerOperationClient {
  private final Map<Long, Boolean> containerCorruptionMap = new HashMap<>();
  private final XceiverClientManager xceiverClientManagerStub;

  public ContainerOperationClientStub(OzoneConfiguration configuration) throws IOException {
    super(configuration);
    this.xceiverClientManagerStub = new XceiverClientManagerStub();
  }

  @Override
  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManagerStub;
  }

  @Override
  public Map<DatanodeDetails, ReadContainerResponseProto> readContainerFromAllNodes(
          long containerID, Pipeline pipeline) throws IOException, InterruptedException {

    if (Boolean.TRUE.equals(containerCorruptionMap.get(containerID))) {
      return Collections.emptyMap();
    }

    ReadContainerResponseProto resp = ReadContainerResponseProto.newBuilder()
            .setContainerData(
                    ContainerProtos.ContainerDataProto.newBuilder()
                            .setContainerID(containerID)
                            .setState(ContainerProtos.ContainerDataProto.State.OPEN)
                            .build()
            )
            .build();

    Map<DatanodeDetails, ReadContainerResponseProto> map = new HashMap<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      map.put(dn, resp);
    }
    return map;
  }
}
