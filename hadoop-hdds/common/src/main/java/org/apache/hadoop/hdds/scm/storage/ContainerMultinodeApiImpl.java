/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link ContainerMultinodeApi} interface.
 * This class provides methods to perform protocol calls on multiple datanodes.
 */
public class ContainerMultinodeApiImpl implements ContainerMultinodeApi {

  private final XceiverClientSpi client;

  private final ContainerApiHelper requestHelper = new ContainerApiHelper();

  public ContainerMultinodeApiImpl(XceiverClientSpi client) {
    this.client = client;
  }

  @Override
  public Map<DatanodeDetails, VerifyBlockResponseProto> verifyBlock(DatanodeBlockID datanodeBlockID,
      Token<OzoneBlockTokenIdentifier> token) throws IOException, InterruptedException {

    String datanodeUuid = client.getPipeline().getFirstNode().getUuidString();

    Map<DatanodeDetails, VerifyBlockResponseProto> datanodeToResponseMap = new HashMap<>();

    ContainerCommandRequestProto request = requestHelper.createVerifyBlockRequest(datanodeBlockID, token, datanodeUuid);
    Map<DatanodeDetails, ContainerCommandResponseProto> responses = client.sendCommandOnAllNodes(request);

    responses.forEach((key, value) -> datanodeToResponseMap.put(key, value.getVerifyBlock()));

    return datanodeToResponseMap;
  }

  @Override
  public void close() throws IOException {
    //client.close();
  }
}
