package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(
    name = "delete",
    description = "Delete a container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "One or more container IDs separated by spaces. " +
      "To read from stdin, specify '-' and supply the container IDs " +
      "separated by newlines.",
      arity = "1..*",
      paramLabel = "<container ID>")
  private String[] containerList;

  @Override
  protected void execute(ScmClient client) throws IOException {
    for (String containerId : containerList) {
      long id = Long.parseLong(containerId);
      client.purgeContainerWithDataBlocks(id);
    }
  }

}
