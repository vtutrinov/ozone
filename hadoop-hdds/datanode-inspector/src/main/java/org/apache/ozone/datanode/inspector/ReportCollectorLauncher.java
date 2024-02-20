package org.apache.ozone.datanode.inspector;

import org.apache.hadoop.hdds.cli.GenericCli;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(subcommands = {DatanodeCommand.class, KeyReport.class})
public class ReportCollectorLauncher extends GenericCli {

  public static void main(String[] args) {
    new CommandLine(new ReportCollectorLauncher()).execute(args);
  }

}