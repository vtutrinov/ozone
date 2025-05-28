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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Set;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.hadoop.ozone.shell.fsck.writer.KeyState.DAMAGED_BLOCKS;
import static org.apache.hadoop.ozone.shell.fsck.writer.KeyState.NO_BLOCKS;
import static org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter.printKeyType;

/** A writer implementation for the Ozone Filesystem Check (Fsck) that outputs information in plain text format. */
public class PlainTextOzoneFsckWriter implements OzoneFsckWriter {
  private final Writer writer;

  public PlainTextOzoneFsckWriter(Writer writer) {
    this.writer = writer;
  }

  @Override
  public void writeKeyInfo(OmKeyInfo keyInfo, PrinterFunction innerInfoPrinter) throws IOException {
    keySeparator();
    printLine("Key Information:");
    printLine("  Name: %s/%s/%s", keyInfo.getVolumeName(), keyInfo.getBucketName(), keyInfo.getKeyName());
    printLine("  Path: %s", keyInfo.getPath());
    printLine("  Size: %s", byteCountToDisplaySize(keyInfo.getDataSize()));
    printLine("  Type: %s", printKeyType(keyInfo));

    innerInfoPrinter.print();

    writer.flush();
  }

  @Override
  public void writeCorruptedKey(OmKeyInfo keyInfo) throws IOException {
    writeKeyInfo(keyInfo, () -> printLine(String.format("Key state: %s! No blocks present for the key.", NO_BLOCKS)));

    writer.flush();
  }

  @Override
  public void writeDamagedBlocks(Set<BlockID> damagedBlocks) throws IOException {
    if (!damagedBlocks.isEmpty()) {
      sectionSeparator();
      printLine("Key state: %s", DAMAGED_BLOCKS);
      printLine("Damaged blocks:");
      for (BlockID blockID : damagedBlocks) {
        printLine("  containerID: %s", blockID.getContainerBlockID().getContainerID());
        printLine("  localID: %s", blockID.getContainerBlockID().getLocalID());
        subsectionSeparator();
      }
    }
  }

  @Override
  public void writeLocationInfo(OmKeyLocationInfo locationInfo) throws IOException {
    subsectionSeparator();
    printLine("Location information:");
    printLine("  Pipeline: %s", locationInfo.getPipeline());
    subsectionSeparator();
  }

  @Override
  public void writeContainerInfo(ContainerDataProto container, DatanodeDetails datanodeDetails,
      PrinterFunction containerDetailsPrinter) throws IOException {
    printLine("  Container %s information:", container.getContainerID());
    printLine("    Path: %s", container.getContainerPath());
    printLine("    Type: %s", container.getContainerType());
    printLine("    State: %s", container.getState());
    printLine("    Datanode: %s (%s)", datanodeDetails.getHostName(), datanodeDetails.getUuidString());

    containerDetailsPrinter.print();
  }

  @Override
  public void writeBlockInfo(BlockData blockInfo, PrinterFunction blockDetailsPrinter) throws IOException {
    subsectionSeparator();
    DatanodeBlockID blockID = blockInfo.getBlockID();

    printLine("  Block %s information:", blockID.getLocalID());
    printLine("    Block commit sequence id: %s", blockID.getBlockCommitSequenceId());

    blockDetailsPrinter.print();
  }

  @Override
  public void writeChunkInfo(List<ChunkInfo> chunks) throws IOException {
    for (ChunkInfo chunk : chunks) {
      printLine("      Chunk: %s", chunk.getChunkName());
    }
  }

  private void eol() throws IOException {
    writer.write(System.lineSeparator());
    writer.flush();
  }

  private void printLine(String line, Object... args) throws IOException {
    writer.write(String.format(line, args));
    eol();
  }

  private void keySeparator() throws IOException {
    separator("============");
  }

  private void sectionSeparator() throws IOException {
    separator("------------");
  }

  private void subsectionSeparator() throws IOException {
    eol();
  }

  private void separator(String pattern) throws IOException {
    writer.write(pattern);
    eol();
  }

  @Override
  public void close() throws IOException {
    writer.flush();
    writer.close();
  }
}
