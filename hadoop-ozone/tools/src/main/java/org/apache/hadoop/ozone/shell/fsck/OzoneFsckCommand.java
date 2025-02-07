/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell.fsck;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.ozone.shell.fsck.writer.JsonOzoneFsckWriter;
import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;
import org.apache.hadoop.ozone.shell.fsck.writer.PlainTextOzoneFsckWriter;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The {@link OzoneFsckCommand} class is a command-line tool for performing file system checks within Ozone.
 * This tool supports various options to fine-tune the file system check process,
 * including filters for volumes, buckets, and keys.
 * It can also produce verbose output detailing keys, containers, blocks, and chunks as well as delete corrupted keys.
 * <p>
 * Options:
 * <ul>
 * <li>`--volume-prefix`: Specifies the prefix for volumes that should be included in the check.
 * <li>`--bucket-prefix`: Specifies the prefix for buckets that should be included in the check.
 * <li>`--key-prefix`: Specifies the prefix for keys that should be included in the check.
 * <li>`--delete`: Deletes the corrupted keys.
 * <li>`--healthy-keys`: Displays information about good and healthy keys.
 * <li>`--verbosity-level`: Controls verbosity of the presented output.
 * <li>`--output` or `-o`: Specifies the file to output information about the scan process.
 * <li>`--output-format`: Specifies what format to use for a report.
 * If not specified, the information will be printed to the system output.
 *</ul>
 * @see OzoneFsckVerbosityLevel OzoneFsckVerbosityLevel class for a verbosity level options.
 * @see OzoneFsckOutputFormat OzoneFsckOutputFormat class for an output format options.
 */
@CommandLine.Command(name = "fscheck",
    description = "Operational tool to run system-wide file check in Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class OzoneFsckCommand extends Handler implements SubcommandWithParent {

  @CommandLine.Option(names = {"--volume-prefix"},
      description = "Specifies the prefix for volumes that should be included in the check")
  private String volumePrefix;

  @CommandLine.Option(names = {"--bucket-prefix"},
      description = "Specifies the prefix for buckets that should be included in the check")
  private String bucketPrefix;

  @CommandLine.Option(names = {"--key-prefix"},
      description = "Specifies the prefix for keys that should be included in the check")
  private String keyPrefix;

  @CommandLine.Option(names = {"--delete"},
      description = "Deletes the corrupted keys")
  private boolean delete;

  @CommandLine.Option(names = {"--healthy-keys"},
      description = "Specifies whether to display information about good and healthy keys")
  private boolean keys;

  @CommandLine.Option(names = {"--verbosity-level"},
      defaultValue = "KEY",
      description = "Controls a verbosity of the presented output."
          + " By default printed information only about a key itself."
          + " Possible values: KEY, CONTAINER, BLOCK, CHUNK.\n"
          + " - KEY: prints information about a key itself only.\n"
          + " - CONTAINER: additionally prints information about key's containers.\n"
          + " - BLOCK: additionally prints information about container's blocks.\n"
          + " - CHUNK: prints information about block's chunk.")
  private OzoneFsckVerbosityLevel verbose;

  @CommandLine.Option(names = {"--output", "-o"},
      description = "Specifies the file to output information about the scan process."
          + " If not specified, the information will be printed to the system output.")
  private String output;

  @CommandLine.Option(names = {"--output-format"},
      defaultValue = "JSON",
      description = "Specifies a format in which output will be written."
          + "Possible values: PLAIN_TEXT, JSON, XML. JSON is a default value.")
  private OzoneFsckOutputFormat outputFormat;

  @CommandLine.Option(names = {"--checkpoint"},
      defaultValue = "/tmp/checkpoint.txt",
      description = "Specifies the path to the checkpoint file. Default: /tmp/checkpoint.txt")
  private String checkpoint;

  @CommandLine.ParentCommand
  private Shell shell;

  @Override
  public boolean isVerbose() {
    return shell.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    return shell.createOzoneConfiguration();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException, OzoneClientException {
    OzoneFsckPathPrefix pathPrefix = new OzoneFsckPathPrefix(volumePrefix, bucketPrefix, keyPrefix);

    OzoneFsckVerboseSettings verboseSettings = OzoneFsckVerboseSettings.builder()
        .printHealthyKeys(keys)
        .level(verbose)
        .build();

    OzoneConfiguration ozoneConfiguration = getConf();
    ContainerOperationClient containerOperationClient = new ContainerOperationClient(ozoneConfiguration);

    try (OzoneFsckWriter writer = createReportWriter(output, outputFormat);
         OzoneFsckHandler handler = new OzoneFsckHandler(
             pathPrefix,
             writer,
             verboseSettings,
             delete,
             client,
             containerOperationClient,
             checkpoint
        )) {
      handler.scan();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Can't execute fscheck command", e);
    }
  }

  private static OzoneFsckWriter createReportWriter(@Nullable String output,
      @Nullable OzoneFsckOutputFormat outputFormat) throws IOException {
    Writer writer = createWriter(output);

    OzoneFsckOutputFormat localOutputFormat = outputFormat == null ? OzoneFsckOutputFormat.PLAIN_TEXT : outputFormat;

    switch (localOutputFormat) {
    case PLAIN_TEXT:
      return new PlainTextOzoneFsckWriter(writer);
    case JSON:
      return JsonOzoneFsckWriter.create(writer);
    case XML:
      // TODO: Currently unsupported due to issues with Jackson XML streaming API.
      throw new UnsupportedOperationException("XML format is not supported yet.");
    default:
      throw new IllegalArgumentException("Unsupported output format: " + localOutputFormat);
    }
  }

  private static Writer createWriter(@Nullable String output) throws IOException {
    if (output != null) {
      Path outputPath = Paths.get(output);
      Path parentFolder = outputPath.getParent();

      if (parentFolder != null && !Files.exists(parentFolder)) {
        Files.createDirectory(parentFolder);
      }

      if ((parentFolder != null && !Files.isWritable(parentFolder)) ||
              (Files.exists(outputPath) && !Files.isWritable(outputPath))) {
        throw new IOException("Can't write to output file: " + output);
      }

      return Files.newBufferedWriter(outputPath);
    } else {
      return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    }
  }
  @Override
  public Class<?> getParentType() {
    return OzoneShell.class;
  }
}
