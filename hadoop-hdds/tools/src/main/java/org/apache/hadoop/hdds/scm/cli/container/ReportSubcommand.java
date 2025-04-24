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
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is the handler to process the container report command.
 */
@CommandLine.Command(
    name = "report",
    description = "Display the container summary report",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReportSubcommand extends ScmSubcommand {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--json"},
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @CommandLine.Option(names = {"-c", "--count"},
      defaultValue = "100",
      description = "Number of container IDs to display for each health state.")
  private int count;

  @CommandLine.Option(names = {"-s", "--state"},
      split = ",",
      description = "Filter the report by one or more health states (separated by comma)"
                  + "(values: ${COMPLETION-CANDIDATES}). Default is all states.")
  private List<ReplicationManagerReport.HealthState> states;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ReplicationManagerReport report = scmClient.getReplicationManagerReport();

    if (json) {
      output(JsonUtils.toJsonStringWithDefaultPrettyPrinter(report));
      return;
    }

    outputHeader(report.getReportTimeStamp());
    blankLine();
    outputContainerStats(report);
    blankLine();
    outputContainerHealthStats(report);
    blankLine();
    outputContainerSamples(report);
  }

  private void outputHeader(long epochMs) {
    Instant reportTime = Instant.ofEpochSecond(epochMs / 1000);
    outputHeading("Container Summary Report generated at " + reportTime);

  }

  private void outputContainerStats(ReplicationManagerReport report) {
    outputHeading("Container State Summary");
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      long stat = report.getStat(state);
      if (stat != -1) {
        output(state + ": " + stat);
      }
    }
  }

  private void outputContainerHealthStats(ReplicationManagerReport report) {
    outputHeading("Container Health Summary");
    for (ReplicationManagerReport.HealthState state
        : ReplicationManagerReport.HealthState.values()) {

      long stat = report.getStat(state);
      if (stat != -1) {
        output(state + ": " + stat);
      }
    }
  }

  private void outputContainerSamples(ReplicationManagerReport report) {
    Iterable<ReplicationManagerReport.HealthState> toShow =
        (states == null)
            ? Arrays.asList(ReplicationManagerReport.HealthState.values())
            : states;
    for (ReplicationManagerReport.HealthState state : toShow) {
      List<ContainerID> containers = report.getSample(state);
      if (!containers.isEmpty()) {
        int samples = Math.min(containers.size(), count);
        output("First " + samples + " " + state + " containers:");
        output(containers
            .stream()
            .limit(samples)
            .map(ContainerID::toString)
            .collect(Collectors.joining(", ")));
        blankLine();
      }
    }
  }

  private void blankLine() {
    System.out.print("\n");
  }

  private void output(String s) {
    System.out.println(s);
  }

  private void outputHeading(String s) {
    output(s);
    for (int i = 0; i < s.length(); i++) {
      System.out.print("=");
    }
    System.out.print("\n");
  }
}
