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

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.ozone.SecurityAuthAgent;
import org.apache.ozone.oauth.NoneTokenValidator;
import org.apache.ozone.oauth.SimpleJsonParser;
import org.apache.ozone.oauth.TokenValidator;
import org.apache.ozone.provider.AuthDataProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Intercepts {@code KerberosAuthenticationHandler.authenticate()} to
 * perform an OAuth 2.0 / OIDC Authorization Code flow with Keycloak
 * instead of SPNEGO Kerberos.
 *
 * <p>Flow:
 * <ol>
 *   <li>Browser hits a protected URL → no session cookie → redirect
 *       to Keycloak's /auth endpoint with state and redirect_uri</li>
 *   <li>User logs in to Keycloak → redirected back to the original
 *       URL with {@code ?code=...&state=...}</li>
 *   <li>We exchange the code for an access token, validate it,
 *       set a session cookie, redirect to the URL without the
 *       code/state params</li>
 *   <li>Subsequent requests carry the cookie → return
 *       {@link AuthenticationToken}</li>
 * </ol>
 */
public class HttpOidcInterceptor {

  private static final String COOKIE_NAME = "OZONE_OAUTH_SESSION";
  private static final long TOKEN_VALIDITY_MS = 3600_000;

  private static final SecureRandom RANDOM = new SecureRandom();

  /** sessionId -> username */
  private static final ConcurrentHashMap<String, Session> SESSIONS =
      new ConcurrentHashMap<>();

  /** state -> original URL (for CSRF protection + return URL) */
  private static final ConcurrentHashMap<String, String> STATES =
      new ConcurrentHashMap<>();

  private static volatile Constructor<?> authTokenCtor;
  private static volatile TokenValidator validator;

  @RuntimeType
  public static Object authenticate(@This Object handler,
      @AllArguments Object[] args) throws Exception {
    Object request = args[0];
    Object response = args[1];

    // Step 0: Service-to-service — Authorization: Bearer <oauth-token>
    String authHeader = getHeader(request, "Authorization");
    if (authHeader != null && authHeader.startsWith("Bearer ")) {
      String token = authHeader.substring("Bearer ".length()).trim();
      try {
        String username = getValidator().validate(token);
        System.out.println(
            "[SecurityAuthAgent] HTTP Bearer auth validated: "
                + username);
        return createAuthToken(handler, username);
      } catch (Exception e) {
        System.err.println(
            "[SecurityAuthAgent] Bearer token rejected: "
                + e.getMessage());
        sendError(response, 401, "Invalid bearer token");
        return null;
      }
    }

    String code = getParam(request, "code");
    String state = getParam(request, "state");

    // Step 3: OAuth callback — exchange code for token
    if (code != null && state != null) {
      return handleCallback(handler, request, response, code, state);
    }

    // Step 4: Check existing session cookie
    String sessionId = getCookie(request, COOKIE_NAME);
    if (sessionId != null) {
      Session session = SESSIONS.get(sessionId);
      if (session != null && !session.isExpired()) {
        return createAuthToken(handler, session.username);
      }
    }

    // Step 1: Browser flow — redirect to Keycloak
    // Only redirect for GET requests; for other methods, return 401
    String httpMethod = (String) invoke(request, "getMethod");
    if (!"GET".equalsIgnoreCase(httpMethod)) {
      response.getClass().getMethod("setHeader",
              String.class, String.class)
          .invoke(response, "WWW-Authenticate", "Bearer");
      sendError(response, 401, "OAuth required");
      return null;
    }
    redirectToKeycloak(request, response);
    return null;
  }

  private static Object handleCallback(Object handler, Object request,
      Object response, String code, String state) throws Exception {
    String originalUrl = STATES.remove(state);
    if (originalUrl == null) {
      sendError(response, 400, "Invalid state parameter");
      return null;
    }

    AuthDataProvider provider = SecurityAuthAgent.getProvider();
    if (provider == null) {
      sendError(response, 500, "OAuth provider not configured");
      return null;
    }

    String tokenUrl = provider.getServerUrl();
    String clientId = provider.getClientId();
    String redirectUri = buildRedirectUri(request);

    String body = "grant_type=authorization_code"
        + "&client_id=" + URLEncoder.encode(clientId, "UTF-8")
        + "&code=" + URLEncoder.encode(code, "UTF-8")
        + "&redirect_uri=" + URLEncoder.encode(redirectUri, "UTF-8");

    String response_body = postForm(tokenUrl, body);
    Map<String, String> tokenResponse =
        SimpleJsonParser.parse(response_body);
    String accessToken = tokenResponse.get("access_token");
    if (accessToken == null) {
      sendError(response, 401,
          "OAuth code exchange failed: " + response_body);
      return null;
    }

    String username = getValidator().validate(accessToken);

    // Create session and set cookie
    String sessionId = UUID.randomUUID().toString();
    SESSIONS.put(sessionId,
        new Session(username,
            System.currentTimeMillis() + TOKEN_VALIDITY_MS));
    setCookie(response, COOKIE_NAME, sessionId);

    // Redirect to original URL (clean — no code/state in URL)
    sendRedirect(response, originalUrl);
    return null;
  }

  private static void redirectToKeycloak(Object request, Object response)
      throws Exception {
    AuthDataProvider provider = SecurityAuthAgent.getProvider();
    if (provider == null) {
      sendError(response, 500, "OAuth provider not configured");
      return;
    }

    String tokenUrl = provider.getServerUrl();
    String clientId = provider.getClientId();
    if (tokenUrl == null || clientId == null) {
      sendError(response, 500, "OAuth not configured");
      return;
    }

    // Derive auth endpoint: replace /token with /auth
    String authUrl = tokenUrl.endsWith("/token")
        ? tokenUrl.substring(0, tokenUrl.length() - 6) + "/auth"
        : tokenUrl + "/auth";

    String redirectUri = buildRedirectUri(request);
    String state = generateState();
    String originalUrl = buildFullUrl(request);
    STATES.put(state, originalUrl);

    String location = authUrl
        + "?response_type=code"
        + "&client_id=" + URLEncoder.encode(clientId, "UTF-8")
        + "&redirect_uri=" + URLEncoder.encode(redirectUri, "UTF-8")
        + "&state=" + URLEncoder.encode(state, "UTF-8")
        + "&scope=" + URLEncoder.encode("openid profile", "UTF-8");

    System.out.println(
        "[SecurityAuthAgent] Redirecting to Keycloak: " + location);
    sendRedirect(response, location);
  }

  /** Build redirect_uri pointing back to the request's path. */
  private static String buildRedirectUri(Object request) throws Exception {
    String scheme = (String) invoke(request, "getScheme");
    String host = (String) invoke(request, "getServerName");
    int port = (int) invoke(request, "getServerPort");
    String path = (String) invoke(request, "getRequestURI");
    String portPart = (("http".equals(scheme) && port == 80)
        || ("https".equals(scheme) && port == 443))
        ? "" : ":" + port;
    return scheme + "://" + host + portPart + path;
  }

  /** Build full URL including query string (minus code/state). */
  private static String buildFullUrl(Object request) throws Exception {
    String base = buildRedirectUri(request);
    String query = (String) invoke(request, "getQueryString");
    if (query == null || query.isEmpty()) {
      return base;
    }
    StringBuilder filtered = new StringBuilder();
    for (String pair : query.split("&")) {
      if (pair.startsWith("code=") || pair.startsWith("state=")) {
        continue;
      }
      if (filtered.length() > 0) {
        filtered.append('&');
      }
      filtered.append(pair);
    }
    return filtered.length() == 0 ? base : base + "?" + filtered;
  }

  private static String generateState() {
    byte[] bytes = new byte[16];
    RANDOM.nextBytes(bytes);
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  private static TokenValidator getValidator() {
    if (validator == null) {
      // Use the same validation mode as SASL
      validator = new NoneTokenValidator();
    }
    return validator;
  }

  private static Object createAuthToken(Object handler, String username)
      throws Exception {
    if (authTokenCtor == null) {
      ClassLoader cl = handler.getClass().getClassLoader();
      Class<?> tokenClass = Class.forName(
          "org.apache.hadoop.security.authentication.server"
              + ".AuthenticationToken",
          true, cl);
      authTokenCtor = tokenClass.getConstructor(
          String.class, String.class, String.class);
    }
    Object token = authTokenCtor.newInstance(
        username, username, HttpAuthInterceptor.AUTH_TYPE);
    token.getClass().getMethod("setExpires", long.class)
        .invoke(token, System.currentTimeMillis() + TOKEN_VALIDITY_MS);
    return token;
  }

  // ---- HTTP helpers using reflection ----

  private static String getParam(Object request, String name)
      throws Exception {
    return (String) request.getClass()
        .getMethod("getParameter", String.class)
        .invoke(request, name);
  }

  private static String getHeader(Object request, String name)
      throws Exception {
    return (String) request.getClass()
        .getMethod("getHeader", String.class)
        .invoke(request, name);
  }

  private static String getCookie(Object request, String name)
      throws Exception {
    Object[] cookies = (Object[]) request.getClass()
        .getMethod("getCookies").invoke(request);
    if (cookies == null) {
      return null;
    }
    for (Object cookie : cookies) {
      String cookieName = (String) cookie.getClass()
          .getMethod("getName").invoke(cookie);
      if (name.equals(cookieName)) {
        return (String) cookie.getClass()
            .getMethod("getValue").invoke(cookie);
      }
    }
    return null;
  }

  private static void setCookie(Object response, String name, String value)
      throws Exception {
    ClassLoader cl = response.getClass().getClassLoader();
    Class<?> cookieClass =
        Class.forName("javax.servlet.http.Cookie", true, cl);
    Constructor<?> ctor =
        cookieClass.getConstructor(String.class, String.class);
    Object cookie = ctor.newInstance(name, value);
    cookieClass.getMethod("setPath", String.class).invoke(cookie, "/");
    cookieClass.getMethod("setHttpOnly", boolean.class)
        .invoke(cookie, true);
    response.getClass().getMethod("addCookie", cookieClass)
        .invoke(response, cookie);
  }

  private static void sendRedirect(Object response, String location)
      throws Exception {
    response.getClass().getMethod("sendRedirect", String.class)
        .invoke(response, location);
  }

  private static void sendError(Object response, int code, String msg)
      throws Exception {
    response.getClass().getMethod("sendError", int.class, String.class)
        .invoke(response, code, msg);
  }

  private static Object invoke(Object obj, String method) throws Exception {
    return obj.getClass().getMethod(method).invoke(obj);
  }

  private static String postForm(String url, String body)
      throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL(url).openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setConnectTimeout(10_000);
      conn.setReadTimeout(10_000);
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

  private static class Session {
    final String username;
    final long expiresAt;

    Session(String username, long expiresAt) {
      this.username = username;
      this.expiresAt = expiresAt;
    }

    boolean isExpired() {
      return System.currentTimeMillis() >= expiresAt;
    }
  }
}
