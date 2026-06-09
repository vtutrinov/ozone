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

/**
 * Minimal OAuth2 client using {@link HttpURLConnection}.
 * Supports password grant and refresh token flows.
 */
public class OAuthClient {

  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int READ_TIMEOUT_MS = 10_000;
  private static final int MAX_RETRIES = 3;
  private static final long RETRY_DELAY_MS = 2_000;

  /**
   * Obtain an access token using the Resource Owner Password grant.
   */
  public OAuthToken obtainToken(String serverUrl, String clientId,
      String login, String password) throws IOException {
    StringBuilder body = new StringBuilder();
    body.append("grant_type=password");
    if (clientId != null && !clientId.isEmpty()) {
      body.append("&client_id=").append(urlEncode(clientId));
    }
    body.append("&username=").append(urlEncode(login));
    body.append("&password=").append(urlEncode(password));
    return executeTokenRequest(serverUrl, body.toString());
  }

  /**
   * Refresh an access token using a refresh token.
   */
  public OAuthToken refreshToken(String serverUrl, String clientId,
      String refreshToken) throws IOException {
    StringBuilder body = new StringBuilder();
    body.append("grant_type=refresh_token");
    if (clientId != null && !clientId.isEmpty()) {
      body.append("&client_id=").append(urlEncode(clientId));
    }
    body.append("&refresh_token=").append(urlEncode(refreshToken));
    return executeTokenRequest(serverUrl, body.toString());
  }

  private OAuthToken executeTokenRequest(String serverUrl, String body)
      throws IOException {
    IOException lastException = null;
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        return doPost(serverUrl, body);
      } catch (IOException e) {
        lastException = e;
        if (attempt < MAX_RETRIES - 1) {
          org.apache.ozone.AgentLog.warn(
              "Token request failed (attempt " + (attempt + 1) + "/"
                  + MAX_RETRIES + "): " + e.getMessage());
          try {
            Thread.sleep(RETRY_DELAY_MS * (attempt + 1));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
    }
    throw lastException;
  }

  private OAuthToken doPost(String serverUrl, String body)
      throws IOException {
    URL url = new URL(serverUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("Content-Type",
          "application/x-www-form-urlencoded");
      conn.setRequestProperty("Accept", "application/json");

      byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
      conn.setFixedLengthStreamingMode(bodyBytes.length);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(bodyBytes);
      }

      int status = conn.getResponseCode();
      String responseBody = readResponse(conn, status);

      if (status != 200) {
        throw new IOException("OAuth token request failed (HTTP "
            + status + "): " + responseBody);
      }

      return parseTokenResponse(responseBody);
    } finally {
      conn.disconnect();
    }
  }

  private String readResponse(HttpURLConnection conn, int status)
      throws IOException {
    InputStream is = status >= 400 ? conn.getErrorStream()
        : conn.getInputStream();
    if (is == null) {
      return "";
    }
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(is, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    }
  }

  private OAuthToken parseTokenResponse(String json) throws IOException {
    Map<String, String> fields = SimpleJsonParser.parse(json);
    String accessToken = fields.get("access_token");
    if (accessToken == null || accessToken.isEmpty()) {
      throw new IOException(
          "No access_token in OAuth response: " + json);
    }
    String refreshToken = fields.get("refresh_token");
    long expiresIn = 300;
    String expiresStr = fields.get("expires_in");
    if (expiresStr != null) {
      try {
        expiresIn = Long.parseLong(expiresStr);
      } catch (NumberFormatException e) {
        // use default
      }
    }
    long expiresAt = System.currentTimeMillis() + expiresIn * 1000;
    return new OAuthToken(accessToken, refreshToken, expiresAt);
  }

  private static String urlEncode(String value) {
    try {
      return URLEncoder.encode(value, "UTF-8");
    } catch (Exception e) {
      return value;
    }
  }
}
