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

package org.apache.hadoop.ozone.admin.om;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Option-matrix tests for ozone admin om rangercache. These cover the
 * client-side validation that runs before any RPC is attempted.
 */
public class TestRangerCacheSubcommand {

  private RangerCacheSubcommand subject;
  private CommandLine cmd;

  @BeforeEach
  public void setup() {
    subject = new RangerCacheSubcommand();
    cmd = new CommandLine(subject);
  }

  private IOException callExpectingValidationError(String... args)
      throws Exception {
    cmd.parseArgs(args);
    return assertThrows(IOException.class, () -> subject.call());
  }

  @Test
  public void requiresExactlyOneOperation() {
    assertThrows(CommandLine.MissingParameterException.class,
        () -> cmd.parseArgs("-id", "omservice"));

    assertThrows(CommandLine.MutuallyExclusiveArgsException.class,
        () -> cmd.parseArgs("--status", "--invalidate"));

    assertThrows(CommandLine.MutuallyExclusiveArgsException.class,
        () -> cmd.parseArgs("--invalidate", "--extend"));
  }

  @Test
  public void policyAndRoleOnlyWithInvalidate() throws Exception {
    IOException e =
        callExpectingValidationError("--status", "--policy", "p1");
    assertTrue(e.getMessage().contains("only be used with --invalidate"));

    e = callExpectingValidationError("--extend", "--ttl", "5m",
        "--role", "r1");
    assertTrue(e.getMessage().contains("only be used with --invalidate"));
  }

  @Test
  public void policyAndRoleAreMutuallyExclusive() throws Exception {
    IOException e = callExpectingValidationError("--invalidate",
        "--policy", "p1", "--role", "r1");
    assertTrue(e.getMessage().contains("mutually exclusive"));
  }

  @Test
  public void extendRequiresTtl() throws Exception {
    IOException e = callExpectingValidationError("--extend");
    assertTrue(e.getMessage().contains("requires --ttl"));
  }

  @Test
  public void ttlOnlyWithExtend() throws Exception {
    IOException e = callExpectingValidationError("--status", "--ttl", "5m");
    assertTrue(e.getMessage().contains("only be used with --extend"));

    e = callExpectingValidationError("--invalidate", "--ttl", "30s");
    assertTrue(e.getMessage().contains("only be used with --extend"));
  }

  @Test
  public void negativeTtlRejected() throws Exception {
    IOException e = callExpectingValidationError("--extend", "--ttl", "-5m");
    assertTrue(e.getMessage().contains("positive"));
  }
}
