package org.apache.ozone.datanode.inspector;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.JSchException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.symmetric.DefaultSecretKeyClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenGenerator;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.shell.common.VolumeBucketUri;
import org.apache.hadoop.security.token.Token;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CommandLine.Command(name = "list-containers", mixinStandardHelpOptions = true)
public class ListContainers extends ScmSubcommand {

  @CommandLine.ParentCommand
  DatanodeCommand parentCommand;

  @CommandLine.Mixin
  private VolumeBucketUri address;

  Pattern patternToMatchContainerListObjects = Pattern.compile("\\{\\r\\n((?:.*?|\\r\\n)*?)\\r\\n\\}",
      Pattern.DOTALL|Pattern.MULTILINE);

  Pattern patternToMatchChunkObjects = Pattern.compile("^.*?\\{(.*)\\}[^}]+$", Pattern.DOTALL|Pattern.MULTILINE);

  Pattern patternToMatchFileNonExistence = Pattern.compile("ls: cannot access.*No such file or directory");

  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      description = "Reports output directory"
  )
  String outDir;

  @CommandLine.Option(
      names = {"--block-page-size", "-bps"},
      description = "Page size to fetch blocks per container",
      type = Integer.class,
      defaultValue = "2147483647"
  )
  Integer blockPageSize;

  @CommandLine.Option(
      names = {"--current-host"},
      description = "Generate the container report for the current node",
      type= Boolean.class,
      defaultValue = "false"
  )
  Boolean currentHost;

  private OzoneConfiguration conf;

  private String outputDir;

  private ScmClient scmClient;

  private ContainerTokenGenerator containerTokenGenerator;

  @Override
  protected void execute(ScmClient client) throws IOException {
    try {
      this.scmClient = client;
      Field scmOption1 = getClass().getSuperclass().getDeclaredField("scmOption");
      scmOption1.setAccessible(true);
      Field spec1 = scmOption1.get(this).getClass().getDeclaredField("spec");
      spec1.setAccessible(true);
      GenericParentCommand parent = (GenericParentCommand)
          ((CommandLine.Model.CommandSpec)(spec1.get(scmOption1.get(this)))).root().userObject();
      conf = parent.createOzoneConfiguration();
      long tokenLifetime = TimeUnit.DAYS.toMillis(1);
      SecretKeyClient secretKeyClient = DefaultSecretKeyClient.create(conf,
          HddsServerUtil.getSecretKeyClientForDatanode(conf), "secret-client-");
      containerTokenGenerator = new ContainerTokenSecretManager(
          tokenLifetime, secretKeyClient);
      secretKeyClient.start(conf);


      List<DatanodeCommand.DatanodeWithAttributes> datanodes = parentCommand.getAllNodes(client);

      outputDir = "./";
      if (outDir != null) {
        outputDir = outDir;
      }
      if (!Files.exists(Paths.get(outputDir))) {
        Files.createDirectories(Paths.get(outputDir));
      }
      if (outputDir.charAt(outputDir.length() - 1) != '/') {
        outputDir += "/";
      }

      try {
        if (currentHost) {
          String hostAddress = InetAddress.getLocalHost().getHostAddress();
          datanodeContainerInfo(datanodes.stream().filter(dnAttributes -> dnAttributes.getDatanodeDetails()
              .getIpAddress().equals(hostAddress)).findFirst().get().getDatanodeDetails());
        } else {
          if (parentCommand.datanodes != null && !parentCommand.datanodes.isEmpty()) {
            for (String datanode : parentCommand.datanodes) {
              Optional<DatanodeCommand.DatanodeWithAttributes> datanodeInfo =
                  datanodes.stream().filter(dn -> dn.getDatanodeDetails().getIpAddress().equals(datanode) ||
                          dn.getDatanodeDetails().getHostName().equals(datanode) ||
                          dn.getDatanodeDetails().getUuidString().equals(datanode))
                      .findFirst();
              if (!datanodeInfo.isPresent()) {
                throw new IllegalArgumentException("Requested datanode='" + datanode + "' not found");
              }
              DatanodeDetails datanodeDetails = datanodeInfo.get().getDatanodeDetails();
              datanodeContainerInfo(datanodeDetails);
            }
          } else {
            for (DatanodeCommand.DatanodeWithAttributes datanodeWithAttributes : parentCommand.getAllNodes(client)) {
              datanodeContainerInfo(datanodeWithAttributes.getDatanodeDetails());
            }
          }
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void datanodeContainerInfo(DatanodeDetails datanodeDetails) throws JSchException, IOException {
    String containerListCommandResult = null;
    if (currentHost) {
      // in case of currentHost=true
      Process exec = Runtime.getRuntime().exec(new String[]{"ozone", "debug", "container", "list"});
      BufferedReader stdInput = new BufferedReader(new
          InputStreamReader(exec.getInputStream()));
      List<String> strings = IOUtils.readLines(stdInput);
      containerListCommandResult = String.join("\r\n", strings);
    } else {
      StringBuilder cmd = parentCommand.buildDebugCommand();
      cmd.append("container list");
      containerListCommandResult = parentCommand.executeCommands(datanodeDetails.getIpAddress(),
          Collections.singletonList(cmd.toString()));

    }
    Matcher matcher = patternToMatchContainerListObjects.matcher(containerListCommandResult);
    while (matcher.find()) {
      String containerInfoJson = "{" + matcher.group(1) + "}";
      JsonObject containerJsonObject = JsonParser.parseString(containerInfoJson).getAsJsonObject();
      String containerId = containerJsonObject.get("containerID").getAsString();
      containerJsonObject.add("blocks", new JsonArray());
      String containerReportDir = outputDir + "datanode_" + datanodeDetails.getIpAddress() + "/container_" + containerId;
      Path containerReportPath = Paths.get(containerReportDir);
      if (!Files.exists(containerReportPath)) {
        Files.createDirectories(containerReportPath);
      }
      try (OutputStream containerReportFile = Files.newOutputStream(Paths.get(containerReportDir + "/container.json"),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
        IOUtils.write(containerJsonObject.toString(), containerReportFile, StandardCharsets.UTF_8);
      }


      Pipeline pipeline = scmClient.getContainerWithPipeline(Long.parseLong(containerId)).getPipeline();
      Token<ContainerTokenIdentifier> containerToken =
          containerTokenGenerator.generateToken(
              "any", new ContainerID(Long.parseLong(containerId)));

      try (OutputStream blocksDataOutputStream = Files.newOutputStream(Paths.get(containerReportDir + "/blocks.json"),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

        ObjectMapper blockDataObjectMapper = new ObjectMapper();
        blockDataObjectMapper.setVisibility(blockDataObjectMapper.getSerializationConfig().getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        JsonGenerator blocksDataJsonGenerator = blockDataObjectMapper.createGenerator(blocksDataOutputStream,
            JsonEncoding.UTF8);
        blocksDataJsonGenerator.writeStartObject();
        blocksDataJsonGenerator.writeFieldName("blocks");
        blocksDataJsonGenerator.writeStartArray();

        try (XceiverClientSpi xceiverClientSpi = ((ContainerOperationClient) scmClient).getXceiverClientManager()
            .acquireClient(createSingleNodePipeline(pipeline, datanodeDetails))) {
          ContainerProtos.ListBlockResponseProto containerBlocks = ContainerProtocolCalls.listBlock(
              xceiverClientSpi, Long.parseLong(containerId), null, blockPageSize, containerToken);
          ContainerProtos.BlockData blockData = null;
          while (containerBlocks.getBlockDataCount() > 0) {
            boolean nextContainer = false;
            for (int i = 0; i < containerBlocks.getBlockDataCount(); i++) {
              if (blockData != null && blockData.equals(containerBlocks.getBlockData(i))) {
                nextContainer = true;
                break;
              }
              blockData = containerBlocks.getBlockData(i);
              blocksDataJsonGenerator.writeStartObject();
              blocksDataJsonGenerator.writeFieldName("blockId");
              blocksDataJsonGenerator.writeStartObject();
              blocksDataJsonGenerator.writeNumberField("containerId", blockData.getBlockID().getContainerID());
              blocksDataJsonGenerator.writeNumberField("localId", blockData.getBlockID().getLocalID());
              blocksDataJsonGenerator.writeNumberField("blockCommitSequenceId", blockData.getBlockID().getBlockCommitSequenceId());
              blocksDataJsonGenerator.writeEndObject();

              blocksDataJsonGenerator.writeFieldName("metadata");
              blocksDataJsonGenerator.writeStartObject();

              for (ContainerProtos.KeyValue blockMetadataEntry: blockData.getMetadataList()) {
                blocksDataJsonGenerator.writeStringField(blockMetadataEntry.getKey(), blockMetadataEntry.getValue());
              }
              blocksDataJsonGenerator.writeEndObject();

              blocksDataJsonGenerator.writeFieldName("chunks");
              blocksDataJsonGenerator.writeStartArray();

              for (ContainerProtos.ChunkInfo chunkInfo: blockData.getChunksList()) {
                blocksDataJsonGenerator.writeStartObject();

                blocksDataJsonGenerator.writeStringField("chunkName", chunkInfo.getChunkName());
                blocksDataJsonGenerator.writeNumberField("offset", chunkInfo.getOffset());
                blocksDataJsonGenerator.writeNumberField("len", chunkInfo.getLen());
                blocksDataJsonGenerator.writeFieldName("checksumData");
                blocksDataJsonGenerator.writeStartObject();
                blocksDataJsonGenerator.writeStringField("type", chunkInfo.getChecksumData().getType().name());
                blocksDataJsonGenerator.writeNumberField("bytesPerChecksum", chunkInfo.getChecksumData().getBytesPerChecksum());
                blocksDataJsonGenerator.writeEndObject();

                blocksDataJsonGenerator.writeEndObject();
              }
              blocksDataJsonGenerator.writeEndArray();
              blocksDataJsonGenerator.writeNumberField("size", blockData.getSize());
              blocksDataJsonGenerator.writeEndObject();
            }
            if (nextContainer) {
              break;
            }
            containerBlocks = ContainerProtocolCalls.listBlock(
                xceiverClientSpi, Long.parseLong(containerId), blockData.getBlockID().getLocalID(), blockPageSize,
                containerToken);
          }
        }
        blocksDataJsonGenerator.writeEndArray();
        blocksDataJsonGenerator.writeEndObject();
        blocksDataJsonGenerator.close();

      }
    }
  }

  private Pipeline createSingleNodePipeline(Pipeline pipeline,
                                            DatanodeDetails node) {
    return Pipeline.newBuilder().setId(pipeline.getId())
        .setReplicationConfig(pipeline.getReplicationConfig())
        .setState(pipeline.getPipelineState())
        .setNodes(ImmutableList.of(node)).build();
  }

}
