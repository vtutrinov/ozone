package org.apache.hadoop.ozone.shell.fsck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.shell.fsck.writer.PrinterFunction;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.fsck.writer.PlainTextOzoneFsckWriter;
import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PlainTextOzoneFsckWriterTest {
  private StringWriter stringWriter;
  private OzoneFsckWriter writer;

  @BeforeEach
  void setUp() {
    stringWriter = new StringWriter();
    writer = new PlainTextOzoneFsckWriter(stringWriter);
  }

  @Test
  void testWriteKeyInfo() throws IOException {
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getVolumeName()).thenReturn("vol1");
    when(keyInfo.getBucketName()).thenReturn("buck1");
    when(keyInfo.getKeyName()).thenReturn("key1");
    when(keyInfo.getPath()).thenReturn("/vol1/buck1/key1");
    when(keyInfo.getDataSize()).thenReturn(1024L);

    PrinterFunction printerFunction = () -> {

    };

    writer.writeKeyInfo(keyInfo, printerFunction);
    String output = stringWriter.toString();

    String expectedPlaintextOutput =
        "============\n" +
        "Key Information:\n" +
        "  Name: vol1/buck1/key1\n" +
        "  Path: /vol1/buck1/key1\n" +
        "  Size: 1 KB\n" +
        "  Type: DIRECTORY\n";

    assertEquals(expectedPlaintextOutput, output);
  }

  @Test
  void testWriteCorruptedKey() throws IOException {
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getVolumeName()).thenReturn("vol2");
    when(keyInfo.getBucketName()).thenReturn("buck2");
    when(keyInfo.getKeyName()).thenReturn("key2");
    when(keyInfo.getPath()).thenReturn("/vol2/buck2/key2");
    when(keyInfo.getDataSize()).thenReturn(2048L);

    writer.writeCorruptedKey(keyInfo);
    String output = stringWriter.toString();

    String expectedPlaintextOutput =
        "============\n" +
        "Key Information:\n" +
        "  Name: vol2/buck2/key2\n" +
        "  Path: /vol2/buck2/key2\n" +
        "  Size: 2 KB\n" +
        "  Type: DIRECTORY\n" +
        "Key state: NO_BLOCKS! No blocks present for the key.\n";

    assertEquals(output, expectedPlaintextOutput);
  }

  @Test
  void testAdditionalOutputs() throws IOException {
    OmKeyLocationInfo locationInfo = mock(OmKeyLocationInfo.class);
    Pipeline pipeline = mock(Pipeline.class);
    when(locationInfo.getPipeline()).thenReturn(pipeline);
    when(pipeline.toString()).thenReturn("pipeline-mock");

    ContainerDataProto container = mock(ContainerDataProto.class);
    when(container.getContainerID()).thenReturn(1L);
    when(container.getContainerPath()).thenReturn("/path/to/container");
    when(container.getContainerType()).thenReturn(ContainerProtos.ContainerType.KeyValueContainer);
    when(container.getState()).thenReturn(ContainerDataProto.State.OPEN);

    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
    when(datanodeDetails.getHostName()).thenReturn("datanode1");
    when(datanodeDetails.getUuidString()).thenReturn("uuid-1234");

    PrinterFunction containerDetailsPrinter = () -> {

    };

    DatanodeBlockID blockID = mock(DatanodeBlockID.class);
    when(blockID.getLocalID()).thenReturn(67890L);
    when(blockID.getBlockCommitSequenceId()).thenReturn(42L);

    BlockData blockData = mock(BlockData.class);
    when(blockData.getBlockID()).thenReturn(blockID);

    PrinterFunction blockDetailsPrinter = () -> {

    };

    ChunkInfo chunk = mock(ChunkInfo.class);
    when(chunk.getChunkName()).thenReturn("chunk1");

    writer.writeLocationInfo(locationInfo);

    writer.writeContainerInfo(container, datanodeDetails, containerDetailsPrinter);

    writer.writeBlockInfo(blockData, blockDetailsPrinter);

    writer.writeChunkInfo(Collections.singletonList(chunk));

    writer.close();
    String output = stringWriter.toString();

    String expectedOutput =
        "\nLocation information:\n" +
        "  Pipeline: pipeline-mock\n" +
        "\n" +
        "  Container 1 information:\n" +
        "    Path: /path/to/container\n" +
        "    Type: KeyValueContainer\n" +
        "    State: OPEN\n" +
        "    Datanode: datanode1 (uuid-1234)\n" +
        "\n" +
        "  Block 67890 information:\n" +
        "    Block commit sequence id: 42\n" +
        "      Chunk: chunk1\n";

    assertEquals(expectedOutput, output);
  }
}
