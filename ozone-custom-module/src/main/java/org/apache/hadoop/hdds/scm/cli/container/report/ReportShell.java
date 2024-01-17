package org.apache.hadoop.hdds.scm.cli.container.report;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.shell.Shell;
import picocli.CommandLine;

/**
 * The ReportShell class represents a shell for the Ozone object report system.
 * It extends the Shell class, which is a command-line interface for executing Ozone functions.
 */
@CommandLine.Command(name = "ozone report",
    description = "Shell for Ozone object report system",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class ReportShell extends Shell {
  public ReportShell() {
    super(ReportShell.class);
  }

  public static void main(String[] argv) throws Exception {
    new ReportShell().run(argv);
  }

  @Override
  public int execute(String[] argv) {
    TracingUtil.initTracing("shell", createOzoneConfiguration());
    String spanName = "ozone report " + String.join(" ", argv);
    return TracingUtil.executeInNewSpan(spanName,
        () -> super.execute(argv));  }
}
