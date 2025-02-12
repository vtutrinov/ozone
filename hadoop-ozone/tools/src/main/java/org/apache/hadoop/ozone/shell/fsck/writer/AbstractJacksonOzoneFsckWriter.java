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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.ozone.shell.fsck.writer.KeyState.DAMAGED_BLOCKS;
import static org.apache.hadoop.ozone.shell.fsck.writer.KeyState.NO_BLOCKS;
import static org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter.formatKeyName;
import static org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter.printKeyType;

/**
 * Abstract writer for Ozone Filesystem Check (Fsck) that provides a base
 * for outputting data in different formats, such as JSON and XML.
 */
public abstract class AbstractJacksonOzoneFsckWriter implements OzoneFsckWriter {
  private final JsonGenerator generator;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  protected AbstractJacksonOzoneFsckWriter(JsonGenerator generator) throws IOException {
    this.generator = generator;

    this.generator.writeStartArray();
  }

  @Override
  public void writeKeyInfo(OmKeyInfo keyInfo, PrinterFunction innerInfoPrinter) throws IOException {
    generator.writeStartObject();

    generator.writeStringField("name", formatKeyName(keyInfo));
    generator.writeStringField("path", keyInfo.getPath());
    generator.writeStringField("size", FileUtils.byteCountToDisplaySize(keyInfo.getDataSize()));
    generator.writeStringField("type", printKeyType(keyInfo));

    innerInfoPrinter.print();

    generator.writeEndObject();
    generator.flush();
  }

  @Override
  public void writeCorruptedKey(OmKeyInfo keyInfo) throws IOException {
    writeKeyInfo(keyInfo, () -> generator.writeStringField("state", String.valueOf(NO_BLOCKS)));
  }

  @Override
  public void writeDamagedBlocks(Set<BlockID> damagedBlocks) throws IOException {
    if (!damagedBlocks.isEmpty()) {
      generator.writeStringField("state", String.valueOf(DAMAGED_BLOCKS));

      generator.writeArrayFieldStart("damaged_blocks");

      for (BlockID blockID : damagedBlocks) {
        generator.writeStartObject();
        generator.writeNumberField("containerID", blockID.getContainerBlockID().getContainerID());
        generator.writeNumberField("localID", blockID.getContainerBlockID().getLocalID());
        generator.writeEndObject();
      }
      generator.writeEndArray();
    }
  }

  @Override
  public void writeLocationInfo(OmKeyLocationInfo locationInfo) throws IOException {
    generator.writeObjectFieldStart("location_info");

    generator.writeStringField("pipeline", locationInfo.getPipeline().toString());

    generator.writeEndObject();
  }

  @Override
  public void writeContainerInfo(ContainerDataProto container, DatanodeDetails datanodeDetails,
      PrinterFunction containerDetailsPrinter) throws IOException {
    generator.writeFieldName("container");

    generator.writeStartObject();

    generator.writeNumberField("id", container.getContainerID());
    generator.writeStringField("path", container.getContainerPath());
    generator.writeStringField("type", container.getContainerType().toString());
    generator.writeStringField("state", container.getState().toString());
    generator.writeStringField("datanode",
        String.format("%s (%s)", datanodeDetails.getHostName(), datanodeDetails.getUuidString()));

    containerDetailsPrinter.print();

    generator.writeEndObject();
  }

  @Override
  public void writeBlockInfo(BlockData blockInfo, PrinterFunction blockDetailsPrinter) throws IOException {
    generator.writeObjectFieldStart("block");

    DatanodeBlockID blockID = blockInfo.getBlockID();

    generator.writeNumberField("id", blockID.getLocalID());
    generator.writeNumberField("sequence_id", blockID.getBlockCommitSequenceId());

    blockDetailsPrinter.print();

    generator.writeEndObject();
  }

  @Override
  public void writeChunkInfo(List<ChunkInfo> chunk) throws IOException {
    generator.writeArrayFieldStart("chunks");

    for (ChunkInfo chunkInfo : chunk) {
      generator.writeString(chunkInfo.getChunkName());
    }

    generator.writeEndArray();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      generator.writeEndArray();
      generator.writeRaw("\n");
      generator.close();
    }
  }
}
