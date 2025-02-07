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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for communication with multiple datanodes.
 * Provides methods to perform any protocol calls by Container clients on multiple datanodes.
 */
public interface ContainerMultinodeApi extends AutoCloseable {
  /**
   * Verifies the specified block on multiple datanodes.
   *
   * @param datanodeBlockID the ID of the block to be verified
   * @param token the security token required for block verification
   * @return a map containing the datanode details and their respective verification response
   * @throws IOException if an I/O error occurs during verification
   * @throws InterruptedException if the verification process is interrupted
   */
  Map<DatanodeDetails, VerifyBlockResponseProto> verifyBlock(DatanodeBlockID datanodeBlockID,
      Token<OzoneBlockTokenIdentifier> token) throws IOException, InterruptedException;
}
