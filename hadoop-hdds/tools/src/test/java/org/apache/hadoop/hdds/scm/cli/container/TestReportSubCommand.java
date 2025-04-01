/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the Container ReportSubCommand class.
 */
public class TestReportSubCommand {

  private ReportSubcommand cmd;
  private static final int SEED = 10;
  private static final int SMALL_REPORT_SIZE = 5;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new ReportSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCorrectValuesAppearInEmptyReport() throws IOException, InterruptedException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.orderContainerManagerReport(ReplicationManagerReport.SAMPLE_LIMIT))
        .thenAnswer(invocation -> new ReplicationManagerReport(ReplicationManagerReport.SAMPLE_LIMIT));

    cmd.execute(scmClient);

    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      Pattern p = Pattern.compile(
          "^" + state.toString() + ": 0$", Pattern.MULTILINE);
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }

    for (ReplicationManagerReport.HealthState state :
        ReplicationManagerReport.HealthState.values()) {
      Pattern p = Pattern.compile(
          "^" + state.toString() + ": 0$", Pattern.MULTILINE);
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testValidJsonOutput() throws IOException, InterruptedException {
    // More complete testing of the Report JSON output is in
    // TestReplicationManagerReport.
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.orderContainerManagerReport(ReplicationManagerReport.SAMPLE_LIMIT))
        .thenAnswer(invocation -> new ReplicationManagerReport(ReplicationManagerReport.SAMPLE_LIMIT));

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json");
    cmd.execute(scmClient);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));

    assertNotNull(json.get("reportTimeStamp"));
    assertNotNull(json.get("stats"));
    assertNotNull(json.get("samples"));
  }

  @Test
  public void testCorrectValuesAppearInReport() throws IOException, InterruptedException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.orderContainerManagerReport(ReplicationManagerReport.SAMPLE_LIMIT))
        .thenAnswer(invocation -> createReport());

    cmd.execute(scmClient);

    int counter = SEED;
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      Pattern p = Pattern.compile(
          "^" + state.toString() + ": " + counter + "$", Pattern.MULTILINE);
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
      counter++;
    }

    counter = SEED;
    for (ReplicationManagerReport.HealthState state :
        ReplicationManagerReport.HealthState.values()) {
      Pattern p = Pattern.compile(
          "^" + state.toString() + ": " + counter + "$", Pattern.MULTILINE);
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());

      // Check the correct samples are returned
      p = Pattern.compile(
          "^First 100 " + state + " containers:\n"
              + containerList(0, counter) + "$", Pattern.MULTILINE);
      m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
      counter++;
    }
  }

  @Test
  public void testFilteredTruncatedReport() throws IOException, InterruptedException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.orderContainerManagerReport(SMALL_REPORT_SIZE))
        .thenAnswer(invocation -> createReport());
    CommandLine c = new CommandLine(cmd);
    final ReplicationManagerReport.HealthState first = ReplicationManagerReport.HealthState.UNDER_REPLICATED;
    final ReplicationManagerReport.HealthState second = ReplicationManagerReport.HealthState.MIS_REPLICATED;
    c.parseArgs(
        "-c ", String.valueOf(SMALL_REPORT_SIZE),
        "-t ", first.toString(), second.toString()
    );
    cmd.execute(scmClient);

    int counter = SEED;
    final String out = outContent.toString(DEFAULT_ENCODING);
    for (ReplicationManagerReport.HealthState state : ReplicationManagerReport.HealthState.values()) {
      Pattern p1 = Pattern.compile("^" + state.toString() + ": " + counter + "$", Pattern.MULTILINE);
      Matcher m1 = p1.matcher(out);
      Pattern p2 = Pattern.compile(
          "^First " + SMALL_REPORT_SIZE +
              " " + state + " containers:\n" + containerList(0, SMALL_REPORT_SIZE) +
              "$", Pattern.MULTILINE);
      Matcher m2 = p2.matcher(out);

      if (state == first || state == second) {
        assertTrue(m1.find());
        assertTrue(m2.find());
      } else {
        assertFalse(m1.find());
        assertFalse(m2.find());
      }

      counter++;
    }
  }

  private ReplicationManagerReport createReport() {
    ReplicationManagerReport report = new ReplicationManagerReport();

    int counter = SEED;
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      for (int i = 0; i < counter; i++) {
        report.increment(state);
      }
      counter++;
    }

    // Add samples
    counter = SEED;
    for (ReplicationManagerReport.HealthState state
        : ReplicationManagerReport.HealthState.values()) {
      for (int i = 0; i < counter; i++) {
        report.incrementAndSample(state, ContainerID.valueOf(i));
      }
      counter++;
    }
    return report;
  }

  private String containerList(int start, int end) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < end; i++) {
      if (i != start) {
        sb.append(", ");
      }
      sb.append("#" + i);
    }
    return sb.toString();
  }

}
