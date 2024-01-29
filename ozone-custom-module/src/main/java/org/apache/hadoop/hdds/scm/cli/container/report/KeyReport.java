package org.apache.hadoop.hdds.scm.cli.container.report;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * This class represents a report of the distribution of keys among containers.
 * It retrieves information about the keys dispersion among containers and writes the report in JSON format.
 */
@CommandLine.Command(name = "keysDistribution",
    description = "returns information about an keys dispersion among containers")
public class KeyReport extends Handler {
  @CommandLine.Mixin
  private KeyReportOptions keyReportOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    try (KeyReportWriter writer = new KeyReportWriter(client, keyReportOptions, getConf(), isVerbose())) {
      writer.writeRootInfo();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
