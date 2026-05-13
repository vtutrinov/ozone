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

import org.apache.ozone.SecurityAuthAgent;
import org.apache.ozone.config.AgentConfig;
import org.apache.ozone.interactive.InteractiveAuthFlow;
import org.apache.ozone.provider.AuthDataProvider;

// NoneTokenValidator is in the same oauth package so no import needed

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages OAuth token lifecycle: acquisition, caching, and refresh.
 * Tokens are cached per user principal name.
 */
public final class OAuthTokenManager {

  private static final ConcurrentHashMap<String, OAuthToken> TOKEN_CACHE =
      new ConcurrentHashMap<>();

  private static final OAuthClient CLIENT = new OAuthClient();

  /** Re-entrancy guard to prevent recursion from OAuth HTTP calls. */
  private static final ThreadLocal<Boolean> IN_PROGRESS =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  /** Holds the current thread's access token for downstream use. */
  private static final ThreadLocal<String> CURRENT_ACCESS_TOKEN =
      new ThreadLocal<>();

  /** Cached UGI for the OAuth-authenticated user (interactive flow). */
  private static volatile Object oauthUgi;

  /**
   * Most recently obtained access token, used as a fallback when the
   * per-thread {@link #CURRENT_ACCESS_TOKEN} is unset. Necessary for
   * call paths spawned by service thread pools (e.g., YARN's
   * {@code DelegationTokenRenewer}) where {@code DoAsInterceptor}
   * may not have run on the current thread, but the JVM itself
   * authenticated as a service principal at startup.
   */
  private static volatile String fallbackAccessToken;

  /** Cached UGI class — captured by DoAsInterceptor on first call. */
  private static volatile Class<?> ugiClass;

  /** Flag — has the UGI credentials been scanned for a bundled OAuth token. */
  private static volatile boolean ugiCredentialsScanned;

  private OAuthTokenManager() {
  }

  /**
   * Get a valid OAuth token for the given user. Obtains a new token
   * or refreshes an expired one as needed.
   *
   * @param user the principal name
   * @return valid OAuth token
   * @throws SecurityException if token cannot be obtained
   */
  public static OAuthToken getToken(String user) {
    if (IN_PROGRESS.get()) {
      // Re-entrant call — return cached token or skip
      OAuthToken cached = TOKEN_CACHE.get(user);
      if (cached != null) {
        return cached;
      }
      return null;
    }

    // If credential bundling is enabled, check if a bundled OAuth
    // token was propagated via YARN credentials (AM/task container
    // case). Otherwise the OAuth flow is purely client-side and
    // YARN propagates Ozone's native delegation tokens instead.
    if (isBundleCredsEnabled()) {
      maybeLoadFromUgiCredentials();
    }

    try {
      IN_PROGRESS.set(Boolean.TRUE);
      OAuthToken token = TOKEN_CACHE.compute(user, (k, existing) -> {
        if (existing != null && !existing.isExpired()) {
          return existing;
        }
        try {
          return obtainOrRefresh(existing);
        } catch (IOException e) {
          throw new SecurityException(
              "Failed to obtain OAuth token for " + user, e);
        }
      });
      if (token != null) {
        CURRENT_ACCESS_TOKEN.set(token.getAccessToken());
        fallbackAccessToken = token.getAccessToken();
        // Bundle into UGI credentials only when explicitly enabled
        // via agent arg auth-bundle-creds=true. By default the
        // standard Hadoop delegation token flow is used (Ozone's
        // OzoneToken is fetched by YARN's O3fsDtFetcher and
        // propagated instead).
        if (isBundleCredsEnabled()) {
          bundleIntoUgiCredentials(token);
        }
      }
      return token;
    } finally {
      IN_PROGRESS.set(Boolean.FALSE);
    }
  }

  private static OAuthToken obtainOrRefresh(OAuthToken existing)
      throws IOException {
    AuthDataProvider provider = SecurityAuthAgent.getProvider();
    if (provider == null) {
      throw new IOException("No AuthDataProvider configured");
    }
    String serverUrl = provider.getServerUrl();
    if (serverUrl == null || serverUrl.isEmpty()) {
      throw new IOException("OAuth server URL not configured");
    }

    String clientId = provider.getClientId();

    // Try refresh first if we have a refresh token
    if (existing != null && existing.getRefreshToken() != null) {
      try {
        return CLIENT.refreshToken(serverUrl, clientId,
            existing.getRefreshToken());
      } catch (IOException e) {
        System.err.println(
            "[SecurityAuthAgent] Refresh failed, re-authenticating: "
                + e.getMessage());
      }
    }

    // Obtain new token with credentials
    if (provider.hasCredentials()) {
      return CLIENT.obtainToken(serverUrl, clientId,
          provider.getLogin(), provider.getPassword());
    }

    // Fall back to interactive flow if no credentials
    AgentConfig config = SecurityAuthAgent.getAgentConfig();
    if (config != null) {
      InteractiveAuthFlow flow =
          new InteractiveAuthFlow(config.isQrEnabled(), clientId,
              config.isOfflineAccess());
      OAuthToken token = flow.execute(serverUrl);
      // After interactive login, replace the JVM's login UGI with
      // the OAuth-authenticated user so getCurrentUser() returns
      // the OAuth user (not the OS user).
      replaceLoginUser(token.getAccessToken());
      return token;
    }

    throw new IOException(
        "No credentials available and no valid refresh token");
  }

  /**
   * Get an access token usable for the current outbound RPC.
   * Prefers the per-thread token (set by {@link DoAsInterceptor}),
   * falls back to the most recently obtained token in the JVM
   * (typically the service's own token from startup).
   */
  public static String getCurrentAccessToken() {
    String token = CURRENT_ACCESS_TOKEN.get();
    return token != null ? token : fallbackAccessToken;
  }

  /** Get the cached OAuth UGI, or null if no interactive login. */
  public static Object getOAuthUgi() {
    return oauthUgi;
  }

  /** Cache the UGI class for later reflection. Called by interceptors. */
  public static void setUgiClass(Class<?> cls) {
    if (ugiClass == null && cls != null) {
      ugiClass = cls;
    }
  }

  /**
   * Replace Hadoop's login UGI with one based on the JWT username
   * from the given access token. Called after interactive auth so
   * that {@code UserGroupInformation.getCurrentUser()} returns the
   * OAuth-authenticated user (not the OS user). Otherwise the
   * server sees a mismatch between the SASL principal and the
   * connection-context user, triggering proxy-user authorization
   * that fails ("user X is not allowed to impersonate Y").
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void replaceLoginUser(String accessToken) {
    try {
      String jwtUser = new NoneTokenValidator().validate(accessToken);
      if (jwtUser == null || jwtUser.isEmpty()) {
        return;
      }
      // Prefer the UGI class captured by DoAsInterceptor — its
      // classloader is guaranteed to be the application classloader
      // where Hadoop classes live. Fall back to thread context CL.
      Class<?> resolvedUgiClass = ugiClass;
      ClassLoader cl;
      if (resolvedUgiClass != null) {
        cl = resolvedUgiClass.getClassLoader();
      } else {
        cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
          cl = OAuthTokenManager.class.getClassLoader();
        }
        resolvedUgiClass = Class.forName(
            "org.apache.hadoop.security.UserGroupInformation",
            true, cl);
      }
      Class<?> authMethodClass = Class.forName(
          "org.apache.hadoop.security.SaslRpcServer$AuthMethod",
          true, cl);
      Object kerberos =
          Enum.valueOf((Class<Enum>) authMethodClass, "KERBEROS");

      java.lang.reflect.Method create = resolvedUgiClass.getMethod(
          "createRemoteUser", String.class, authMethodClass);
      Object ugi = create.invoke(null, jwtUser, kerberos);

      java.lang.reflect.Method setLoginUser =
          resolvedUgiClass.getDeclaredMethod("setLoginUser",
              resolvedUgiClass);
      setLoginUser.setAccessible(true);
      setLoginUser.invoke(null, ugi);

      // Cache the OAuth UGI so the connection-context interceptor
      // can substitute it when an existing ticket holds the OS user.
      oauthUgi = ugi;

      System.out.println(
          "[SecurityAuthAgent] Replaced login UGI with OAuth user: "
              + jwtUser);
    } catch (Exception e) {
      System.err.println(
          "[SecurityAuthAgent] Failed to replace login UGI: "
              + e.getMessage());
    }
  }

  /** Clear all cached tokens. Useful for testing. */
  public static void clearCache() {
    TOKEN_CACHE.clear();
    CURRENT_ACCESS_TOKEN.remove();
  }

  // ---- YARN credential propagation (reflection-only) ----

  private static final String OAUTH_TOKEN_KIND = "OAUTH-CREDS";
  private static final String OAUTH_TOKEN_SERVICE = "oauth-creds";

  /**
   * Scan the current user's UGI credentials for an OAUTH-CREDS Token
   * previously bundled by the job submitter. If found, seed the
   * token cache and fallback so subsequent SASL handshakes use it
   * — no need to call Keycloak in the container.
   */
  private static void maybeLoadFromUgiCredentials() {
    if (ugiCredentialsScanned) {
      return;
    }
    synchronized (OAuthTokenManager.class) {
      if (ugiCredentialsScanned) {
        return;
      }
      ugiCredentialsScanned = true;
      try {
        Class<?> ugi = resolveUgiClass();
        if (ugi == null) {
          return;
        }
        Object current = ugi.getMethod("getCurrentUser").invoke(null);
        if (current == null) {
          return;
        }
        Object tokens = current.getClass()
            .getMethod("getTokens").invoke(current);
        if (!(tokens instanceof Iterable)) {
          return;
        }
        for (Object token : (Iterable<?>) tokens) {
          Object kind = token.getClass()
              .getMethod("getKind").invoke(token);
          if (kind == null
              || !OAUTH_TOKEN_KIND.equals(kind.toString())) {
            continue;
          }
          byte[] idBytes = (byte[]) token.getClass()
              .getMethod("getIdentifier").invoke(token);
          byte[] pwBytes = (byte[]) token.getClass()
              .getMethod("getPassword").invoke(token);
          OAuthCredentialsCodec.IdentifierFields id =
              OAuthCredentialsCodec.decodeIdentifierFromTokenForm(idBytes);
          OAuthCredentialsCodec.PasswordFields pw =
              OAuthCredentialsCodec.decodePassword(pwBytes);
          OAuthToken oauth = new OAuthToken(
              pw.accessToken, pw.refreshToken, id.expiresAt);
          TOKEN_CACHE.put(id.username, oauth);
          fallbackAccessToken = pw.accessToken;
          CURRENT_ACCESS_TOKEN.set(pw.accessToken);
          System.out.println(
              "[SecurityAuthAgent] Loaded bundled OAuth token "
                  + "from UGI credentials for user " + id.username);
          return;
        }
      } catch (Throwable t) {
        // Not running in a Hadoop-credentials context (no UGI),
        // or no bundled token. Silent fallback.
      }
    }
  }

  /**
   * Bundle the freshly obtained {@link OAuthToken} into the current
   * UGI's credentials so YARN's job submitter serializes it into
   * the {@code ApplicationSubmissionContext} (the same mechanism
   * Hadoop uses for delegation tokens).
   */
  private static void bundleIntoUgiCredentials(OAuthToken token) {
    if (token == null || token.getAccessToken() == null) {
      return;
    }
    try {
      AuthDataProvider provider = SecurityAuthAgent.getProvider();
      String serverUrl = provider != null ? provider.getServerUrl() : "";
      String clientId = provider != null ? provider.getClientId() : "";
      String username = new NoneTokenValidator()
          .validate(token.getAccessToken());

      Class<?> ugi = resolveUgiClass();
      if (ugi == null) {
        return;
      }
      Object current = ugi.getMethod("getCurrentUser").invoke(null);
      if (current == null) {
        return;
      }

      ClassLoader cl = ugi.getClassLoader();
      Class<?> textClass = Class.forName(
          "org.apache.hadoop.io.Text", true, cl);
      Class<?> hadoopTokenClass = Class.forName(
          "org.apache.hadoop.security.token.Token", true, cl);

      byte[] idBytes = OAuthCredentialsCodec.encodeIdentifierForTokenForm(
          username, serverUrl, clientId, token.getExpiresAt());
      byte[] pwBytes = OAuthCredentialsCodec.encodePassword(
          token.getAccessToken(),
          token.getRefreshToken() == null ? "" : token.getRefreshToken());
      Object kind = textClass.getConstructor(String.class)
          .newInstance(OAUTH_TOKEN_KIND);
      Object service = textClass.getConstructor(String.class)
          .newInstance(OAUTH_TOKEN_SERVICE);
      Object hadoopToken = hadoopTokenClass
          .getConstructor(byte[].class, byte[].class, textClass, textClass)
          .newInstance(idBytes, pwBytes, kind, service);
      current.getClass().getMethod("addToken", hadoopTokenClass)
          .invoke(current, hadoopToken);
      System.out.println(
          "[SecurityAuthAgent] Bundled OAUTH-CREDS token into UGI "
              + "credentials for user " + username);
    } catch (Throwable t) {
      System.err.println(
          "[SecurityAuthAgent] Could not bundle OAuth token into UGI: "
              + t.getMessage());
    }
  }

  private static boolean isBundleCredsEnabled() {
    AgentConfig config = SecurityAuthAgent.getAgentConfig();
    return config != null && config.isBundleCreds();
  }

  private static Class<?> resolveUgiClass() {
    if (ugiClass != null) {
      return ugiClass;
    }
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = OAuthTokenManager.class.getClassLoader();
      }
      return Class.forName(
          "org.apache.hadoop.security.UserGroupInformation", true, cl);
    } catch (Throwable t) {
      return null;
    }
  }
}
