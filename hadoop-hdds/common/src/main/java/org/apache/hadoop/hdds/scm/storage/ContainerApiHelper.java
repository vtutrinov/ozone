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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockRequestProto;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.VerifyBlock;

/**
 * Class designed working with Datanode Proto requests and responses.
 */
class ContainerApiHelper {
  /**
  * Creates a request to verify a block on the datanode.
  *
  * @param datanodeBlockID The identifier for the block on the datanode.
  * @param token The security token used for authentication.
  * @param datanodeUuid The unique identifier of the datanode.
  * @return A {@link ContainerCommandRequestProto} object representing the verify block request.
  * @throws IOException If an I/O error occurs during the request creation.
  */
  ContainerCommandRequestProto createVerifyBlockRequest(DatanodeBlockID datanodeBlockID,
      Token<OzoneBlockTokenIdentifier> token, String datanodeUuid) throws IOException {

    VerifyBlockRequestProto.Builder verifyBlockRequestBuilder = ContainerProtos.VerifyBlockRequestProto
            .newBuilder()
            .setBlockID(datanodeBlockID);

    ContainerCommandRequestProto.Builder commandRequestBuilder = ContainerCommandRequestProto
            .newBuilder()
            .setCmdType(VerifyBlock)
            .setContainerID(datanodeBlockID.getContainerID())
            .setDatanodeUuid(datanodeUuid)
            .setVerifyBlock(verifyBlockRequestBuilder);

    if (token != null) {
      commandRequestBuilder.setEncodedToken(token.encodeToUrlString());
    }

    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      commandRequestBuilder.setTraceID(traceId);
    }

    return commandRequestBuilder.build();
  }
}
