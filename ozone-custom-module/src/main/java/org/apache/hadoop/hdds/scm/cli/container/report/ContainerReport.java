package org.apache.hadoop.hdds.scm.cli.container.report;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.container.ContainerCommands;
import org.apache.hadoop.ozone.shell.Shell;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

import java.util.concurrent.Callable;

/**
 * Represents container specific reports for the CLI.
 */
@CommandLine.Command(
    name = "container",
    description = "Container specific reports",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        KeyReport.class
    })
@MetaInfServices(SubcommandWithParent.class)
public class ContainerReport implements GenericParentCommand, Callable<Void>, SubcommandWithParent {
  @CommandLine.ParentCommand
  private Shell shell;

  @CommandLine.Spec
  private CommandSpec spec;

  @Override
  public Class<?> getParentType() {
    return ReportShell.class;
  }

  @Override
  public Void call() {
    throw new MissingSubcommandException(
        this.shell.getCmd().getSubcommands().get("report"));
  }

  @Override
  public boolean isVerbose() {
    return shell.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    return shell.createOzoneConfiguration();
  }
}
