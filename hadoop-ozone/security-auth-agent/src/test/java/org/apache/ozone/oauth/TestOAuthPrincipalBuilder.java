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

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOAuthPrincipalBuilder {

  @Test
  void deriveRealmFromKeycloakIssuer() {
    assertEquals("OZONE", OAuthPrincipalBuilder.deriveRealm(
        "http://keycloak:8080/realms/ozone"));
  }

  @Test
  void deriveRealmTrailingSlashNormalized() {
    assertEquals("OZONE", OAuthPrincipalBuilder.deriveRealm(
        "http://keycloak:8080/realms/ozone/"));
  }

  @Test
  void deriveRealmFallbackWhenIssuerMissing() {
    assertEquals("OAUTH", OAuthPrincipalBuilder.deriveRealm(null));
    assertEquals("OAUTH", OAuthPrincipalBuilder.deriveRealm(""));
  }

  @Test
  void deriveRealmUppercasesSegment() {
    assertEquals("MYREALM", OAuthPrincipalBuilder.deriveRealm(
        "https://idp.example.com/auth/realms/MyRealm"));
  }

  @Test
  void buildHonorsConfigOverridesEvenWhenIssuerPresent() throws Exception {
    String token = jwt("{\"preferred_username\":\"alice\","
        + "\"iss\":\"http://keycloak/realms/ozone\"}");
    String principal = OAuthPrincipalBuilder.build(token,
        "host.example.com", "FORCED.REALM");
    assertEquals("alice/host.example.com@FORCED.REALM", principal);
  }

  @Test
  void buildDerivesRealmFromIssWhenNoOverride() throws Exception {
    String token = jwt("{\"preferred_username\":\"bob\","
        + "\"iss\":\"http://keycloak/realms/prod\"}");
    String principal = OAuthPrincipalBuilder.build(token,
        "h.example.com", null);
    assertEquals("bob/h.example.com@PROD", principal);
  }

  @Test
  void buildUsesSubWhenNoPreferredUsername() throws Exception {
    String token = jwt("{\"sub\":\"service-account-bot\","
        + "\"iss\":\"http://idp/realms/ops\"}");
    String principal = OAuthPrincipalBuilder.build(token,
        "h.x", null);
    assertEquals("service-account-bot/h.x@OPS", principal);
  }

  @Test
  void buildFailsWhenNoUserClaim() {
    String token = jwt("{\"iss\":\"http://idp/realms/x\"}");
    assertThrows(java.io.IOException.class, () ->
        OAuthPrincipalBuilder.build(token, "h.x", null));
  }

  @Test
  void buildResolvesLocalHostWhenNoOverride() throws Exception {
    String token = jwt("{\"preferred_username\":\"u\","
        + "\"iss\":\"http://idp/realms/r\"}");
    String principal = OAuthPrincipalBuilder.build(token, null, null);
    // Don't assert on the exact hostname (depends on test env), just
    // confirm the three components are present and ordered.
    assertTrue(principal.startsWith("u/"),
        "expected user prefix, got: " + principal);
    assertTrue(principal.endsWith("@R"),
        "expected realm suffix, got: " + principal);
    assertTrue(principal.indexOf('/') < principal.indexOf('@'),
        "host part missing between '/' and '@': " + principal);
  }

  /** Build a tokenless-but-parseable JWT: header.payload.signature. */
  private static String jwt(String payloadJson) {
    String header = b64("{\"alg\":\"none\"}");
    String payload = b64(payloadJson);
    return header + "." + payload + ".sig";
  }

  private static String b64(String s) {
    return Base64.getUrlEncoder().withoutPadding()
        .encodeToString(s.getBytes());
  }
}
