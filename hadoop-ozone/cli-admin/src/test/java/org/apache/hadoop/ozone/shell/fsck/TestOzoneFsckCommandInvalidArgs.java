/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell.fsck;

import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

class TestOzoneFsckCommandInvalidArgs {

  private static Stream<Arguments> invalidArgs() {
    return Stream.of(
            arguments((Object) new String[]{}),
            arguments((Object) new String[]{"--bucket-prefix=bucket"})
    );
  }

  @ParameterizedTest
  @MethodSource("invalidArgs")
  void testFsckInvalidArgs(String[] args) throws Exception {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    try {
      System.setErr(new PrintStream(errContent, true, StandardCharsets.UTF_8.name()));
      CommandLine cmdLine = new CommandLine(new OzoneFsckCommand());
      cmdLine.execute(args);

      String actualErr = errContent.toString(StandardCharsets.UTF_8.name());
      assertTrue(actualErr.contains("Missing required option: '--volume-prefix=<volumePrefix>'"));
      assertTrue(actualErr.contains("Usage: fscheck [-hV] [--delete]"));
      assertTrue(actualErr.contains("--bucket-prefix=<bucketPrefix>\n" +
              "                          Specifies the prefix for buckets that should be\n" +
              "                            included in the check"));
    } finally {
      System.setErr(originalErr);
    }
  }
}
