/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAgentLog {

  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream capturedOut;
  private ByteArrayOutputStream capturedErr;
  private AgentLog.Level originalLevel;

  @BeforeEach
  void redirectStreams() {
    originalOut = System.out;
    originalErr = System.err;
    capturedOut = new ByteArrayOutputStream();
    capturedErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(capturedOut));
    System.setErr(new PrintStream(capturedErr));
    originalLevel = AgentLog.getLevel();
  }

  @AfterEach
  void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    AgentLog.setLevel(originalLevel.name());
  }

  @Test
  void infoLevelEmitsInfoAndAboveOnly() {
    AgentLog.setLevel("INFO");
    AgentLog.debug("d");
    AgentLog.info("i");
    AgentLog.warn("w");
    AgentLog.error("e");
    String out = capturedOut.toString();
    String err = capturedErr.toString();
    assertFalse(out.contains("] d"), "DEBUG should be suppressed at INFO");
    assertTrue(out.contains("] i"), "INFO should be emitted to stdout");
    assertTrue(err.contains("] w"), "WARN should be emitted to stderr");
    assertTrue(err.contains("] e"), "ERROR should be emitted to stderr");
  }

  @Test
  void offSilencesEverything() {
    AgentLog.setLevel("OFF");
    AgentLog.debug("d");
    AgentLog.info("i");
    AgentLog.warn("w");
    AgentLog.error("e");
    assertEquals("", capturedOut.toString());
    assertEquals("", capturedErr.toString());
  }

  @Test
  void debugEmitsEverything() {
    AgentLog.setLevel("DEBUG");
    AgentLog.debug("d");
    AgentLog.info("i");
    AgentLog.warn("w");
    AgentLog.error("e");
    assertTrue(capturedOut.toString().contains("] d"));
    assertTrue(capturedOut.toString().contains("] i"));
    assertTrue(capturedErr.toString().contains("] w"));
    assertTrue(capturedErr.toString().contains("] e"));
  }

  @Test
  void errorOnlyEmitsErrors() {
    AgentLog.setLevel("ERROR");
    AgentLog.debug("d");
    AgentLog.info("i");
    AgentLog.warn("w");
    AgentLog.error("e");
    assertEquals("", capturedOut.toString());
    String err = capturedErr.toString();
    assertFalse(err.contains("] w"), "WARN suppressed at ERROR");
    assertTrue(err.contains("] e"));
  }

  @Test
  void invalidLevelKeepsCurrent() {
    AgentLog.setLevel("WARN");
    AgentLog.setLevel("BOGUS");
    assertEquals(AgentLog.Level.WARN, AgentLog.getLevel());
  }

  @Test
  void caseInsensitiveLevel() {
    AgentLog.setLevel("debug");
    assertEquals(AgentLog.Level.DEBUG, AgentLog.getLevel());
  }

  @Test
  void prefixPresentOnEveryLine() {
    AgentLog.setLevel("DEBUG");
    AgentLog.info("hello");
    assertTrue(capturedOut.toString().startsWith("[SecurityAuthAgent] "),
        "expected agent prefix, got: " + capturedOut);
  }
}
