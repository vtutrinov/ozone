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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Builds a Kerberos-shaped principal ({@code service/host@REALM})
 * from JWT claims and local hostname so the UGI we install carries a
 * name that Hadoop's {@code KerberosName} parser accepts without
 * fallback. This is the "make OAuth identity look Kerberos-shaped"
 * path discussed as Option 2 alongside the
 * {@link org.apache.ozone.interceptor.KerberosNameHostInterceptor}
 * safety net.
 *
 * <p>Inputs in priority order:
 * <ol>
 *   <li>Short username — JWT {@code preferred_username}, else
 *       {@code sub}.</li>
 *   <li>Host — agent arg {@code auth-kerberos-host=}, else the local
 *       canonical hostname.</li>
 *   <li>Realm — agent arg {@code auth-kerberos-realm=}, else derived
 *       from the JWT {@code iss} claim (last URL path segment,
 *       uppercased), else {@code OAUTH}.</li>
 * </ol>
 */
public final class OAuthPrincipalBuilder {

  private static final String DEFAULT_REALM = "OAUTH";

  private OAuthPrincipalBuilder() {
  }

  /**
   * Produce {@code shortUser/host@REALM} for the given access token.
   *
   * @param accessToken JWT access token (signature not verified)
   * @param configHost  optional host override; pass {@code null} to
   *                    use the local canonical hostname
   * @param configRealm optional realm override; pass {@code null} to
   *                    derive from the {@code iss} claim
   * @return full Kerberos-shaped principal
   * @throws IOException if the token can't be decoded or has no
   *                     username claim
   */
  public static String build(String accessToken, String configHost,
      String configRealm) throws IOException {
    Map<String, String> claims =
        NoneTokenValidator.parseClaims(accessToken);
    String shortUser = claims.get("preferred_username");
    if (shortUser == null) {
      shortUser = claims.get("sub");
    }
    if (shortUser == null || shortUser.isEmpty()) {
      throw new IOException("JWT has no preferred_username or sub");
    }
    String host = configHost != null && !configHost.isEmpty()
        ? configHost : localHost();
    String realm = configRealm != null && !configRealm.isEmpty()
        ? configRealm : deriveRealm(claims.get("iss"));
    return shortUser + "/" + host + "@" + realm;
  }

  /** Visible for tests. */
  public static String deriveRealm(String issuer) {
    if (issuer == null || issuer.isEmpty()) {
      return DEFAULT_REALM;
    }
    String trimmed = issuer;
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int idx = trimmed.lastIndexOf('/');
    String tail = idx >= 0 ? trimmed.substring(idx + 1) : trimmed;
    if (tail.isEmpty()) {
      return DEFAULT_REALM;
    }
    return tail.toUpperCase();
  }

  private static String localHost() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return "localhost";
    }
  }
}
