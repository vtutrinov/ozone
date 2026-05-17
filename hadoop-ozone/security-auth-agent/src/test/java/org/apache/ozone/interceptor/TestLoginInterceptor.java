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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestLoginInterceptor {

  @Test
  void substituteHostTokenLeavesAlreadyResolvedPrincipal() {
    String resolved = "om/om.example.com@EXAMPLE.COM";
    assertEquals(resolved,
        LoginInterceptor.substituteHostToken(resolved));
  }

  @Test
  void substituteHostTokenLeavesShortPrincipal() {
    assertEquals("hadoop",
        LoginInterceptor.substituteHostToken("hadoop"));
  }

  @Test
  void substituteHostTokenLeavesUserAtRealm() {
    assertEquals("testuser@EXAMPLE.COM",
        LoginInterceptor.substituteHostToken(
            "testuser@EXAMPLE.COM"));
  }

  @Test
  void substituteHostTokenHandlesNull() {
    assertNull(LoginInterceptor.substituteHostToken(null));
  }

  @Test
  void substituteHostTokenReplacesPlaceholder() {
    String out = LoginInterceptor.substituteHostToken(
        "om/_HOST@EXAMPLE.COM");
    assertNotNull(out);
    assertFalse(out.contains("_HOST"),
        "expected _HOST to be substituted, got: " + out);
    // Shape: service / <host> @ REALM, host non-empty.
    int slash = out.indexOf('/');
    int at = out.indexOf('@');
    assertEquals("om", out.substring(0, slash));
    assertEquals("EXAMPLE.COM", out.substring(at + 1));
    String host = out.substring(slash + 1, at);
    assertFalse(host.isEmpty(),
        "host part should not be empty, got: " + out);
  }
}
