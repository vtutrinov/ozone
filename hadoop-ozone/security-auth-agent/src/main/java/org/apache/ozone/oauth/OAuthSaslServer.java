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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * SASL server that accepts an OAuth access token instead of a
 * Kerberos/GSSAPI token. Validates the token using the configured
 * {@link TokenValidator} and extracts the authenticated username.
 */
public class OAuthSaslServer implements SaslServer {

  private final TokenValidator validator;
  private boolean complete;
  private String authorizationId;

  public OAuthSaslServer(TokenValidator validator) {
    this.validator = validator;
  }

  @Override
  public String getMechanismName() {
    return "GSSAPI";
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    String payload = new String(response, StandardCharsets.UTF_8);
    int sep = payload.indexOf(OAuthSaslClient.SEPARATOR);
    String claimedPrincipal;
    String token;
    if (sep > 0) {
      claimedPrincipal = payload.substring(0, sep);
      token = payload.substring(sep + 1);
    } else {
      // Backward-compat: token only, no principal
      claimedPrincipal = null;
      token = payload;
    }

    try {
      String validatedUser = validator.validate(token);

      // If client claimed a principal, verify the JWT username
      // matches its primary (prevents spoofing a different identity).
      if (claimedPrincipal != null) {
        String primary = extractPrimary(claimedPrincipal);
        if (!validatedUser.equals(primary)) {
          throw new SaslException(
              "Claimed principal '" + claimedPrincipal
                  + "' does not match JWT user '" + validatedUser
                  + "'");
        }
        authorizationId = claimedPrincipal;
      } else {
        authorizationId = validatedUser;
      }

      complete = true;
      System.out.println(
          "[SecurityAuthAgent] SASL OAuth validated: "
              + authorizationId + " (jwt user: " + validatedUser + ")");
      return null; // no challenge to send back
    } catch (IOException e) {
      throw new SaslException("OAuth token validation failed", e);
    }
  }

  /**
   * Extract the primary component from a Kerberos principal name.
   * E.g., "om/om@EXAMPLE.COM" → "om", "user@REALM" → "user".
   */
  private static String extractPrimary(String principal) {
    int slash = principal.indexOf('/');
    int at = principal.indexOf('@');
    if (slash > 0) {
      return principal.substring(0, slash);
    }
    if (at > 0) {
      return principal.substring(0, at);
    }
    return principal;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public String getAuthorizationID() {
    return authorizationId;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len)
      throws SaslException {
    throw new SaslException("Unwrap not supported");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len)
      throws SaslException {
    throw new SaslException("Wrap not supported");
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    if ("javax.security.sasl.qop".equals(propName)) {
      return "auth";
    }
    return null;
  }

  @Override
  public void dispose() throws SaslException {
    // no-op
  }
}
