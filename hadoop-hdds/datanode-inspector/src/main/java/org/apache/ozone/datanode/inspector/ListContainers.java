package org.apache.ozone.datanode.inspector;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.JSchException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CommandLine.Command(name = "list-containers", mixinStandardHelpOptions = true)
public class ListContainers extends ScmSubcommand {

  @CommandLine.ParentCommand
  DatanodeCommand parentCommand;

  Pattern patternToMatchContainerListObjects = Pattern.compile("\\{\\r\\n((?:.*?|\\r\\n)*?)\\r\\n\\}",
      Pattern.DOTALL|Pattern.MULTILINE);

  Pattern patternToMatchChunkObjects = Pattern.compile("^.*?\\{(.*)\\}[^}]+$", Pattern.DOTALL|Pattern.MULTILINE);

  Pattern patternToMatchFileNonExistence = Pattern.compile("ls: cannot access.*No such file or directory");

  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      description = "Reports output directory"
  )
  String outDir;

  @Override
  protected void execute(ScmClient client) throws IOException {
    try {
      if (parentCommand.datanodes != null && !parentCommand.datanodes.isEmpty()) {
        for (String datanode : parentCommand.datanodes) {
          datanodeContainerInfo(datanode);
        }
      } else {
        for (DatanodeCommand.DatanodeWithAttributes datanodeWithAttributes : parentCommand.getAllNodes(client)) {
          String ipAddress = datanodeWithAttributes.getDatanodeDetails().getIpAddress();
          datanodeContainerInfo(ipAddress);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void datanodeContainerInfo(String host) throws JSchException, IOException {
    StringBuilder cmd = parentCommand.buildDebugCommand();
    cmd.append("container list");
    String result = parentCommand.executeCommands(host, Collections.singletonList(cmd.toString()));

    Matcher matcher = patternToMatchContainerListObjects.matcher(result);
    Set<String> dbFiles = new HashSet<>();
    JsonObject containers = new JsonObject();
    while (matcher.find()) {
      String containerInfoJson = "{" + matcher.group(1) + "}";
      JsonObject jsonObject = JsonParser.parseString(containerInfoJson).getAsJsonObject();
      jsonObject.add("blocks", new JsonArray());
      containers.add(jsonObject.get("containerID").getAsString(), jsonObject);
      dbFiles.add(jsonObject.get("dbFile").getAsString());
    }
    if (!dbFiles.isEmpty()) {
      for (String dbFile: dbFiles) {
        cmd = parentCommand.buildDebugCommand();
        cmd.append("ldb --db=").append(dbFile).append(" scan --cf=block_data");
        result = parentCommand.executeCommands(host, Collections.singletonList(cmd.toString()));
        matcher = patternToMatchChunkObjects.matcher(result);
        while (matcher.find()) {
          String chunksObject = "{" + matcher.group(1) + "}";
          JsonObject blocks = JsonParser.parseString(chunksObject).getAsJsonObject();
          for (Map.Entry<String, JsonElement> block: blocks.entrySet()) {
            String[] keyElements = block.getKey().split("|");
            String containerId = keyElements[0];
            JsonElement blockObject = block.getValue();
            String blockLocalId = blockObject.getAsJsonObject().getAsJsonObject("blockID")
                .getAsJsonObject("containerBlockID").get("localID").getAsString();
            JsonObject containerObject = containers.getAsJsonObject(containerId);
            String blockFilePath = containerObject.get("chunksPath").getAsString() + "/" + blockLocalId + ".block";
            blockObject.getAsJsonObject().addProperty("blockFilePath", blockFilePath);
            result = parentCommand.executeCommands(host, Collections.singletonList("ls -lah " + blockFilePath));
            boolean blockFileExists = !patternToMatchFileNonExistence.matcher(result).find();
            blockObject.getAsJsonObject().addProperty("blockFileExists", blockFileExists);
            containers.getAsJsonObject(containerId).getAsJsonArray("blocks").add(blockObject);
          }
        }
      }
    }
    if (containers.size() > 0) {
      String outputDir = "./";
      if (outDir != null) {
        outputDir = outDir;
      }
      if (!Files.exists(Paths.get(outputDir))) {
        Files.createDirectories(Paths.get(outputDir));
      }
      String hostContainersReportFileName = outputDir + "datanode_" + host + ".json";
      Path hostContainersReportFile = Files.createFile(Paths.get(hostContainersReportFileName));
      IOUtils.write(containers.toString(),
          Files.newOutputStream(hostContainersReportFile), StandardCharsets.UTF_8);
    }
  }

}
