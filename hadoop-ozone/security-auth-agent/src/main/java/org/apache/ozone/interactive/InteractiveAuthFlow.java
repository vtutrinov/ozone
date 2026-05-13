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
package org.apache.ozone.interactive;

import org.apache.ozone.oauth.OAuthClient;
import org.apache.ozone.oauth.OAuthToken;
import org.apache.ozone.oauth.SimpleJsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Implements the OAuth 2.0 Device Authorization Grant (RFC 8628)
 * for interactive authentication when no credentials are configured.
 *
 * <p>Flow:
 * <ol>
 *   <li>POST to device authorization endpoint</li>
 *   <li>Display verification URL and user code to stderr</li>
 *   <li>Optionally display QR code</li>
 *   <li>Poll token endpoint until user completes auth</li>
 *   <li>Persist obtained token to session file</li>
 * </ol>
 */
public class InteractiveAuthFlow {

  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int READ_TIMEOUT_MS = 10_000;
  private static final long DEFAULT_POLL_INTERVAL_MS = 5_000;
  private static final long MAX_WAIT_MS = 300_000; // 5 minutes
  private static final String SESSION_FILE = ".hadoop-auth/session";

  private final boolean qrEnabled;
  private final String clientId;
  private final boolean offlineAccess;

  public InteractiveAuthFlow(boolean qrEnabled) {
    this(qrEnabled, "ozone-client", false);
  }

  public InteractiveAuthFlow(boolean qrEnabled, String clientId) {
    this(qrEnabled, clientId, false);
  }

  public InteractiveAuthFlow(boolean qrEnabled, String clientId,
      boolean offlineAccess) {
    this.qrEnabled = qrEnabled;
    this.clientId = clientId == null ? "ozone-client" : clientId;
    this.offlineAccess = offlineAccess;
  }

  /**
   * Run the device authorization flow.
   *
   * @param serverUrl the base OAuth server URL (token endpoint)
   * @return obtained OAuth token
   * @throws IOException if the flow fails
   */
  public OAuthToken execute(String serverUrl) throws IOException {
    // Try to load a saved session first
    OAuthToken saved = loadSession();
    if (saved != null && !saved.isExpired()) {
      return saved;
    }

    // Saved session is expired — try the refresh token before
    // re-prompting the user for an interactive login.
    if (saved != null && saved.getRefreshToken() != null) {
      try {
        System.err.println(
            "[SecurityAuthAgent] Saved session expired — refreshing");
        OAuthToken refreshed = new OAuthClient().refreshToken(
            serverUrl, clientId, saved.getRefreshToken());
        saveSession(refreshed);
        return refreshed;
      } catch (IOException e) {
        System.err.println(
            "[SecurityAuthAgent] Refresh failed, falling back to "
                + "device flow: " + e.getMessage());
      }
    }

    // Derive device authorization endpoint from token URL
    String deviceUrl = deriveDeviceUrl(serverUrl);

    // Step 1: Request device code
    Map<String, String> deviceResponse = requestDeviceCode(deviceUrl);
    String deviceCode = deviceResponse.get("device_code");
    String userCode = deviceResponse.get("user_code");
    String verificationUri = deviceResponse.get("verification_uri");
    if (verificationUri == null) {
      verificationUri =
          deviceResponse.get("verification_uri_complete");
    }

    if (deviceCode == null || verificationUri == null) {
      throw new IOException(
          "Invalid device authorization response: " + deviceResponse);
    }

    long interval = DEFAULT_POLL_INTERVAL_MS;
    String intervalStr = deviceResponse.get("interval");
    if (intervalStr != null) {
      try {
        interval = Long.parseLong(intervalStr) * 1000;
      } catch (NumberFormatException e) {
        // use default
      }
    }

    // Step 2: Display instructions to user
    System.err.println();
    System.err.println(
        "========================================");
    System.err.println(
        "  To authenticate, visit:");
    System.err.println("  " + verificationUri);
    if (userCode != null) {
      System.err.println();
      System.err.println("  Enter code: " + userCode);
    }
    System.err.println(
        "========================================");
    System.err.println();

    // Step 2b: Optional QR code
    if (qrEnabled && verificationUri != null) {
      String qr = QrCodeGenerator.generate(verificationUri);
      if (qr != null) {
        System.err.println(qr);
        System.err.println();
      }
    }

    // Step 3: Poll for token
    OAuthToken token = pollForToken(serverUrl, deviceCode, interval);

    // Step 4: Persist session
    saveSession(token);

    return token;
  }

  private Map<String, String> requestDeviceCode(String deviceUrl)
      throws IOException {
    StringBuilder body = new StringBuilder();
    body.append("client_id=").append(URLEncoder.encode(clientId, "UTF-8"));
    if (offlineAccess) {
      body.append("&scope=").append(
          URLEncoder.encode("openid offline_access", "UTF-8"));
    }
    String response = post(deviceUrl, body.toString());
    return SimpleJsonParser.parse(response);
  }

  private OAuthToken pollForToken(String serverUrl, String deviceCode,
      long intervalMs) throws IOException {
    long deadline = System.currentTimeMillis() + MAX_WAIT_MS;

    while (System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(intervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted during device auth", e);
      }

      String body = "grant_type="
          + URLEncoder.encode(
          "urn:ietf:params:oauth:grant-type:device_code", "UTF-8")
          + "&device_code=" + URLEncoder.encode(deviceCode, "UTF-8")
          + "&client_id=" + URLEncoder.encode(clientId, "UTF-8");

      try {
        String response = post(serverUrl, body);
        Map<String, String> fields = SimpleJsonParser.parse(response);

        String error = fields.get("error");
        if (error != null) {
          switch (error) {
          case "authorization_pending":
            continue;
          case "slow_down":
            intervalMs += 5000;
            continue;
          default:
            throw new IOException(
                "Device auth failed: " + error + " - "
                    + fields.get("error_description"));
          }
        }

        String accessToken = fields.get("access_token");
        if (accessToken != null) {
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
          long expiresAt =
              System.currentTimeMillis() + expiresIn * 1000;
          return new OAuthToken(accessToken, refreshToken, expiresAt);
        }
      } catch (IOException e) {
        // HTTP error during polling — retry
        System.err.println(
            "[SecurityAuthAgent] Poll error: " + e.getMessage());
      }
    }
    throw new IOException(
        "Device authorization timed out after " + MAX_WAIT_MS + "ms");
  }

  private String post(String url, String body) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL(url).openConnection();
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
      InputStream is = status >= 400 ? conn.getErrorStream()
          : conn.getInputStream();
      if (is == null) {
        if (status >= 400) {
          throw new IOException("HTTP " + status + " with no body");
        }
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
    } finally {
      conn.disconnect();
    }
  }

  private String deriveDeviceUrl(String tokenUrl) {
    // Common pattern: replace /token with /auth/device
    // For Keycloak: /protocol/openid-connect/token ->
    //              /protocol/openid-connect/auth/device
    if (tokenUrl.endsWith("/token")) {
      return tokenUrl.substring(0, tokenUrl.length() - 6)
          + "/auth/device";
    }
    return tokenUrl + "/device";
  }

  private OAuthToken loadSession() {
    Path sessionPath = Paths.get(
        System.getProperty("user.home"), SESSION_FILE);
    if (!Files.exists(sessionPath)) {
      return null;
    }
    try (BufferedReader reader =
             Files.newBufferedReader(sessionPath, StandardCharsets.UTF_8)) {
      String accessToken = null;
      String refreshToken = null;
      long expiresAt = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        int eq = line.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String key = line.substring(0, eq).trim();
        String value = line.substring(eq + 1).trim();
        switch (key) {
        case "access_token":
          accessToken = value;
          break;
        case "refresh_token":
          refreshToken = value;
          break;
        case "expires_at":
          try {
            expiresAt = Long.parseLong(value);
          } catch (NumberFormatException e) {
            // ignore
          }
          break;
        default:
          break;
        }
      }
      if (accessToken != null) {
        return new OAuthToken(accessToken, refreshToken, expiresAt);
      }
    } catch (IOException e) {
      // ignore — will re-authenticate
    }
    return null;
  }

  private void saveSession(OAuthToken token) {
    Path sessionPath = Paths.get(
        System.getProperty("user.home"), SESSION_FILE);
    try {
      Files.createDirectories(sessionPath.getParent());
      try (BufferedWriter writer =
               Files.newBufferedWriter(sessionPath,
                   StandardCharsets.UTF_8)) {
        writer.write("access_token=" + token.getAccessToken());
        writer.newLine();
        if (token.getRefreshToken() != null) {
          writer.write("refresh_token=" + token.getRefreshToken());
          writer.newLine();
        }
        writer.write("expires_at=" + token.getExpiresAt());
        writer.newLine();
      }
    } catch (IOException e) {
      System.err.println(
          "[SecurityAuthAgent] Failed to save session: "
              + e.getMessage());
    }
  }
}
