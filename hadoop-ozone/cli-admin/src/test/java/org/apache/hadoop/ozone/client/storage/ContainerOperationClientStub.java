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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

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
