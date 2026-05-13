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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOAuthClientIntegration {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;

    server.createContext("/token", exchange -> {
      String response = "{"
          + "\"access_token\":\"test-access-token\","
          + "\"refresh_token\":\"test-refresh-token\","
          + "\"expires_in\":300,"
          + "\"token_type\":\"Bearer\""
          + "}";
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type",
          "application/json");
      exchange.sendResponseHeaders(200, bytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(bytes);
      }
    });

    server.createContext("/error", exchange -> {
      String response = "{"
          + "\"error\":\"invalid_grant\","
          + "\"error_description\":\"Bad credentials\""
          + "}";
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type",
          "application/json");
      exchange.sendResponseHeaders(401, bytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(bytes);
      }
    });

    server.createContext("/refresh", exchange -> {
      String body = readStream(exchange.getRequestBody());
      if (body.contains("grant_type=refresh_token")) {
        String response = "{"
            + "\"access_token\":\"refreshed-access-token\","
            + "\"refresh_token\":\"new-refresh-token\","
            + "\"expires_in\":600,"
            + "\"token_type\":\"Bearer\""
            + "}";
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type",
            "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(bytes);
        }
      } else {
        exchange.sendResponseHeaders(400, 0);
        exchange.getResponseBody().close();
      }
    });

    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private static String readStream(InputStream is) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int len;
    while ((len = is.read(buf)) != -1) {
      bos.write(buf, 0, len);
    }
    return bos.toString("UTF-8");
  }

  @Test
  void testObtainTokenSuccess() throws IOException {
    OAuthClient client = new OAuthClient();
    OAuthToken token = client.obtainToken(
        baseUrl + "/token", "test-client", "user", "pass");

    assertNotNull(token);
    assertEquals("test-access-token", token.getAccessToken());
    assertEquals("test-refresh-token", token.getRefreshToken());
    assertTrue(token.getExpiresAt() > System.currentTimeMillis());
  }

  @Test
  void testObtainTokenFailure() {
    OAuthClient client = new OAuthClient();
    IOException ex = assertThrows(IOException.class, () ->
        client.obtainToken(baseUrl + "/error", "test-client",
            "user", "bad"));
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  void testRefreshTokenSuccess() throws IOException {
    OAuthClient client = new OAuthClient();
    OAuthToken token = client.refreshToken(
        baseUrl + "/refresh", "test-client", "old-refresh-token");

    assertNotNull(token);
    assertEquals("refreshed-access-token", token.getAccessToken());
    assertEquals("new-refresh-token", token.getRefreshToken());
  }
}
