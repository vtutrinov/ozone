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
package org.apache.ozone.interceptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal parser for TOML-subset config files with {@code [section]}
 * headers and {@code key=value} lines.
 */
public final class SimpleConfigParser {

  private SimpleConfigParser() {
  }

  /**
   * Parse a config file into a map of section name to key-value pairs.
   * Lines starting with {@code #} are comments. Blank lines are ignored.
   */
  public static Map<String, Map<String, String>> parse(Path path)
      throws IOException {
    Map<String, Map<String, String>> sections = new LinkedHashMap<>();
    String currentSection = "";
    try (BufferedReader reader =
             Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        if (line.startsWith("[") && line.endsWith("]")) {
          currentSection = line.substring(1, line.length() - 1).trim();
          sections.putIfAbsent(currentSection, new LinkedHashMap<>());
          continue;
        }
        int eq = line.indexOf('=');
        if (eq > 0) {
          String key = line.substring(0, eq).trim();
          String value = line.substring(eq + 1).trim();
          sections.computeIfAbsent(currentSection,
              k -> new LinkedHashMap<>()).put(key, value);
        }
      }
    }
    return Collections.unmodifiableMap(sections);
  }

  /** Get a value from parsed sections, returning null if not found. */
  public static String getValue(Map<String, Map<String, String>> sections,
      String section, String key) {
    Map<String, String> sec = sections.get(section);
    return sec != null ? sec.get(key) : null;
  }
}
