package org.apache.ozone.datanode.inspector;

import com.jcraft.jsch.JSchException;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.util.Collections;

@Command(name = "inspect-containers", mixinStandardHelpOptions = true)
public class InspectContainers extends ScmSubcommand {

  @ParentCommand
  DatanodeCommand parentCommand;

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
    cmd.append("container inspect");
    String result = parentCommand.executeCommands(host, Collections.singletonList(cmd.toString()));
    System.out.println(result);
  }

}
