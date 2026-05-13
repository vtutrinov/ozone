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
package org.apache.ozone.oauth;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal JSON parser for flat OAuth token responses.
 * Only handles flat objects with string and numeric values.
 */
public final class SimpleJsonParser {

  private SimpleJsonParser() {
  }

  /**
   * Parse a flat JSON object like
   * {@code {"access_token":"abc","expires_in":300}}.
   *
   * @return map of key to string representation of value
   */
  public static Map<String, String> parse(String json) {
    Map<String, String> result = new LinkedHashMap<>();
    if (json == null) {
      return result;
    }
    json = json.trim();
    if (json.startsWith("{")) {
      json = json.substring(1);
    }
    if (json.endsWith("}")) {
      json = json.substring(0, json.length() - 1);
    }

    int i = 0;
    int len = json.length();
    while (i < len) {
      i = skipWhitespace(json, i, len);
      if (i >= len) {
        break;
      }

      // parse key
      if (json.charAt(i) != '"') {
        i++;
        continue;
      }
      int keyStart = i + 1;
      int keyEnd = json.indexOf('"', keyStart);
      if (keyEnd < 0) {
        break;
      }
      String key = json.substring(keyStart, keyEnd);
      i = keyEnd + 1;

      // skip colon
      i = skipWhitespace(json, i, len);
      if (i >= len || json.charAt(i) != ':') {
        break;
      }
      i++;
      i = skipWhitespace(json, i, len);
      if (i >= len) {
        break;
      }

      // parse value
      char c = json.charAt(i);
      if (c == '"') {
        // string value
        int valStart = i + 1;
        int valEnd = findUnescapedQuote(json, valStart);
        if (valEnd < 0) {
          break;
        }
        result.put(key, unescape(json.substring(valStart, valEnd)));
        i = valEnd + 1;
      } else if (c == 'n' && json.startsWith("null", i)) {
        i += 4;
      } else if (c == 't' && json.startsWith("true", i)) {
        result.put(key, "true");
        i += 4;
      } else if (c == 'f' && json.startsWith("false", i)) {
        result.put(key, "false");
        i += 5;
      } else {
        // numeric value
        int valStart = i;
        while (i < len && json.charAt(i) != ',' && json.charAt(i) != '}'
            && !Character.isWhitespace(json.charAt(i))) {
          i++;
        }
        result.put(key, json.substring(valStart, i).trim());
      }

      // skip comma
      i = skipWhitespace(json, i, len);
      if (i < len && json.charAt(i) == ',') {
        i++;
      }
    }
    return result;
  }

  private static int skipWhitespace(String s, int i, int len) {
    while (i < len && Character.isWhitespace(s.charAt(i))) {
      i++;
    }
    return i;
  }

  private static int findUnescapedQuote(String s, int from) {
    int i = from;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (c == '\\') {
        i += 2;
        continue;
      }
      if (c == '"') {
        return i;
      }
      i++;
    }
    return -1;
  }

  private static String unescape(String s) {
    if (s.indexOf('\\') < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char next = s.charAt(i + 1);
        switch (next) {
        case '"':
        case '\\':
        case '/':
          sb.append(next);
          break;
        case 'n':
          sb.append('\n');
          break;
        case 't':
          sb.append('\t');
          break;
        case 'r':
          sb.append('\r');
          break;
        default:
          sb.append(c);
          sb.append(next);
          break;
        }
        i++;
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
