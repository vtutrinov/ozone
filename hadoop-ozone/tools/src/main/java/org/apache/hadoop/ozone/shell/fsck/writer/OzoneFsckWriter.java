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

package org.apache.hadoop.ozone.shell.fsck.writer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import static org.apache.hadoop.ozone.shell.fsck.writer.KeyType.DIRECTORY;
import static org.apache.hadoop.ozone.shell.fsck.writer.KeyType.FILE;

/**
 * An interface for writing various information from the Ozone File System Check (Fsck) operation.
 * It provides methods to write information about keys, corrupted keys,
 * damaged blocks, location info, container info, block info, and chunk info.
 * <p>
 * Classes implementing this interface need to handle the details of how this information is serialized and written,
 * typically to a file or some other output stream.
 */
public interface OzoneFsckWriter extends AutoCloseable {
  /**
   * Writes the key information to the output.
   *
   * @param keyInfo The key information to be written.
   * @param innerInfoPrinter A functional interface to handle the printing of an additional details about the key.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeKeyInfo(OmKeyInfo keyInfo, PrinterFunction innerInfoPrinter) throws IOException;

  /**
   * Writes the information of a corrupted key to the output.
   *
   * @param keyInfo The information of the corrupted key to be written.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeCorruptedKey(OmKeyInfo keyInfo) throws IOException;

  /**
   * Writes the information of damaged blocks to the output.
   *
   * @param damagedBlocks A set of {@link BlockID} of damaged blocks.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeDamagedBlocks(Set<BlockID> damagedBlocks) throws IOException;

  /**
   * Writes the location information of a key to the output.
   *
   * @param locationInfo The {@link OmKeyLocationInfo} object containing the location information to be written.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeLocationInfo(OmKeyLocationInfo locationInfo) throws IOException;

  /**
   * Writes the container information to the output.
   *
   * @param container The container data to be written.
   * @param datanodeDetails The details of the datanode associated with the container.
   * @param containerDetailsPrinter A functional interface
   *   to handle the printing of additional details about the container.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeContainerInfo(ContainerDataProto container, DatanodeDetails datanodeDetails,
                          PrinterFunction containerDetailsPrinter) throws IOException;

  /**
   * Writes the block information to the output.
   *
   * @param blockInfo The {@link BlockData} object containing the block information to be written.
   * @param blockDetailsPrinter A functional interface to handle the printing of additional details about the block.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeBlockInfo(BlockData blockInfo, PrinterFunction blockDetailsPrinter) throws IOException;

  /**
   * Writes the chunk information to the output.
   *
   * @param chunk A list of {@link ChunkInfo} objects containing the information about each chunk to be written.
   * @throws IOException If an I/O error occurs during the writing process.
   */
  void writeChunkInfo(List<ChunkInfo> chunk) throws IOException;

  /**
   * Formats the key information into a string in the form of "volume/bucket/key".
   *
   * @param keyInfo The {@link OmKeyInfo} object containing the key's volume name, bucket name, and key name.
   * @return A formatted string representing the full key name.
   */
  static String formatKeyName(OmKeyInfo keyInfo) {
    return String.format(
        "%s/%s/%s",
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getKeyName()
    );
  }

  /**
   * Determines and returns the type of the provided key (either FILE or DIRECTORY).
   *
   * @param keyInfo The key information for which the type is to be determined.
   * @return A string representing the type of the key:
   *     "FILE" if the key is a file, "DIRECTORY" if the key is a directory.
   */
  static String printKeyType(OmKeyInfo keyInfo) {
    return keyInfo.isFile() ? FILE.toString() : DIRECTORY.toString();
  }

  @Override
  void close() throws IOException;
}
