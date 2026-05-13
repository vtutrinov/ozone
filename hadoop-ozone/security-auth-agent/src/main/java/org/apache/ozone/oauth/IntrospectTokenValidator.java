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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates tokens by calling the OAuth server's token introspection
 * endpoint (RFC 7662). Caches results briefly to reduce load.
 */
public class IntrospectTokenValidator implements TokenValidator {

  private static final int TIMEOUT_MS = 10_000;
  private static final long CACHE_TTL_MS = 30_000;

  private final String introspectUrl;
  private final String clientId;
  private final ConcurrentHashMap<String, CacheEntry> cache =
      new ConcurrentHashMap<>();

  /**
   * @param tokenUrl the OAuth token endpoint URL
   * @param clientId the OAuth client ID
   */
  public IntrospectTokenValidator(String tokenUrl, String clientId) {
    // Keycloak: /protocol/openid-connect/token →
    //           /protocol/openid-connect/token/introspect
    this.introspectUrl = tokenUrl + "/introspect";
    this.clientId = clientId;
  }

  @Override
  public String validate(String accessToken) throws IOException {
    CacheEntry cached = cache.get(accessToken);
    if (cached != null && !cached.isExpired()) {
      return cached.username;
    }

    String body = "token=" + URLEncoder.encode(accessToken, "UTF-8")
        + "&client_id=" + URLEncoder.encode(clientId, "UTF-8");

    URL url = new URL(introspectUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("Content-Type",
          "application/x-www-form-urlencoded");

      byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
      conn.setFixedLengthStreamingMode(bodyBytes.length);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(bodyBytes);
      }

      int status = conn.getResponseCode();
      InputStream is = status >= 400 ? conn.getErrorStream()
          : conn.getInputStream();
      if (is == null) {
        throw new IOException("Introspection failed: HTTP " + status);
      }
      String response;
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        response = sb.toString();
      }

      if (status != 200) {
        throw new IOException(
            "Introspection failed (HTTP " + status + "): " + response);
      }

      Map<String, String> fields = SimpleJsonParser.parse(response);
      if (!"true".equals(fields.get("active"))) {
        throw new IOException("Token is not active");
      }
      String username = fields.get("username");
      if (username == null) {
        username = fields.get("sub");
      }
      if (username == null) {
        throw new IOException("No username in introspection response");
      }

      cache.put(accessToken,
          new CacheEntry(username,
              System.currentTimeMillis() + CACHE_TTL_MS));
      return username;
    } finally {
      conn.disconnect();
    }
  }

  private static class CacheEntry {
    final String username;
    final long expiresAt;

    CacheEntry(String username, long expiresAt) {
      this.username = username;
      this.expiresAt = expiresAt;
    }

    boolean isExpired() {
      return System.currentTimeMillis() >= expiresAt;
    }
  }
}
