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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSimpleJsonParser {

  @Test
  void testParseStandardTokenResponse() {
    String json = "{\"access_token\":\"eyJhbGciOiJSUzI1NiJ9\","
        + "\"token_type\":\"bearer\","
        + "\"expires_in\":300,"
        + "\"refresh_token\":\"eyJhbGciOiJIUzI1NiJ9\"}";

    Map<String, String> result = SimpleJsonParser.parse(json);

    assertEquals("eyJhbGciOiJSUzI1NiJ9",
        result.get("access_token"));
    assertEquals("bearer", result.get("token_type"));
    assertEquals("300", result.get("expires_in"));
    assertEquals("eyJhbGciOiJIUzI1NiJ9",
        result.get("refresh_token"));
  }

  @Test
  void testParseWithNullValue() {
    String json = "{\"access_token\":\"abc\","
        + "\"refresh_token\":null}";

    Map<String, String> result = SimpleJsonParser.parse(json);

    assertEquals("abc", result.get("access_token"));
    assertNull(result.get("refresh_token"));
  }

  @Test
  void testParseBooleanValues() {
    String json = "{\"active\":true,\"blocked\":false}";

    Map<String, String> result = SimpleJsonParser.parse(json);

    assertEquals("true", result.get("active"));
    assertEquals("false", result.get("blocked"));
  }

  @Test
  void testParseEscapedStrings() {
    String json = "{\"msg\":\"hello \\\"world\\\"\"}";

    Map<String, String> result = SimpleJsonParser.parse(json);

    assertEquals("hello \"world\"", result.get("msg"));
  }

  @Test
  void testParseNull() {
    Map<String, String> result = SimpleJsonParser.parse(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testParseEmptyObject() {
    Map<String, String> result = SimpleJsonParser.parse("{}");
    assertTrue(result.isEmpty());
  }

  @Test
  void testParseWithWhitespace() {
    String json = "{ \"key\" : \"value\" , \"num\" : 42 }";

    Map<String, String> result = SimpleJsonParser.parse(json);

    assertEquals("value", result.get("key"));
    assertEquals("42", result.get("num"));
  }
}
