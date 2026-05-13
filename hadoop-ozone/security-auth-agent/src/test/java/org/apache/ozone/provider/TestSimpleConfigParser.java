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
package org.apache.ozone.provider;

import org.apache.ozone.interceptor.SimpleConfigParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestSimpleConfigParser {

  @TempDir
  Path tempDir;

  @Test
  void parseStandardConfig() throws IOException {
    Path file = tempDir.resolve("config");
    Files.write(file, (
        "[auth]\n"
            + "server_url=https://oauth.example.com/token\n"
            + "\n"
            + "[cred]\n"
            + "login=testuser\n"
            + "password=my-secret-password\n"
    ).getBytes(StandardCharsets.UTF_8));

    Map<String, Map<String, String>> sections =
        SimpleConfigParser.parse(file);

    assertEquals("https://oauth.example.com/token",
        SimpleConfigParser.getValue(sections, "auth", "server_url"));
    assertEquals("testuser",
        SimpleConfigParser.getValue(sections, "cred", "login"));
    assertEquals("my-secret-password",
        SimpleConfigParser.getValue(sections, "cred", "password"));
  }

  @Test
  void parseWithCommentsAndBlanks() throws IOException {
    Path file = tempDir.resolve("config");
    Files.write(file, (
        "# This is a comment\n"
            + "\n"
            + "[auth]\n"
            + "# server URL\n"
            + "server_url=https://auth.example.com/token\n"
            + "\n"
            + "[cred]\n"
            + "# empty section\n"
    ).getBytes(StandardCharsets.UTF_8));

    Map<String, Map<String, String>> sections =
        SimpleConfigParser.parse(file);

    assertEquals("https://auth.example.com/token",
        SimpleConfigParser.getValue(sections, "auth", "server_url"));
    assertNull(
        SimpleConfigParser.getValue(sections, "cred", "login"));
  }

  @Test
  void valueWithEqualsSign() throws IOException {
    Path file = tempDir.resolve("config");
    Files.write(file, (
        "[auth]\n"
            + "server_url=https://example.com/token?param=value\n"
    ).getBytes(StandardCharsets.UTF_8));

    Map<String, Map<String, String>> sections =
        SimpleConfigParser.parse(file);

    assertEquals("https://example.com/token?param=value",
        SimpleConfigParser.getValue(sections, "auth", "server_url"));
  }
}
