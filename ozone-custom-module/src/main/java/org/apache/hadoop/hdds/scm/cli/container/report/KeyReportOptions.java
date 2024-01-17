package org.apache.hadoop.hdds.scm.cli.container.report;

import picocli.CommandLine;

/**
 * The KeyReportOptions class represents the options for generating a key report.
 */
public class KeyReportOptions {
  @CommandLine.Option(names = {"--output-file", "-o"},
      description = "Path where report will be stored.",
      required = true)
  private String output;

  @CommandLine.Option(names = {"--volume-prefix"},
      description = "Prefix to filter volumes.")
  private String volumePrefix;

  @CommandLine.Option(names = {"--bucket-prefix"},
      description = "Prefix to filter buckets.")
  private String bucketPrefix;

  @CommandLine.Option(names = {"--key-prefix"},
      description = "Prefix to filter keys.")
  private String keyPrefix;

  public String getOutput() {
    return output;
  }

  public String getVolumePrefix() {
    return volumePrefix;
  }

  public String getBucketPrefix() {
    return bucketPrefix;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }
}
