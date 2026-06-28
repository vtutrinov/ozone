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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

  /**
   * Lazy-initialised single-threaded scheduler that drives proactive
   * token refresh. Created only when {@code auth-token-renewal} is
   * {@code proactive} or {@code both} — keeps idle JVMs free of an
   * extra daemon thread.
   */
  private static volatile ScheduledExecutorService REFRESH_SCHEDULER;

  /**
   * Per-user pending proactive-refresh future. When a new token is
   * obtained for a user the previous future is cancelled so we hold
   * at most one scheduled refresh per user.
   */
  private static final ConcurrentHashMap<String, ScheduledFuture<?>>
      REFRESH_FUTURES = new ConcurrentHashMap<>();

  /**
   * Buffer before {@code expiresAt} at which proactive refresh fires.
   * Chosen so the new token is in hand well before the in-flight one
   * expires, even on slow Keycloak round-trips.
   */
  private static final long PROACTIVE_REFRESH_BUFFER_MS = 30_000L;

  /** Minimum delay between proactive refreshes — clamp the schedule. */
  private static final long PROACTIVE_REFRESH_MIN_DELAY_MS = 1_000L;

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
        // Schedule a background refresh ahead of expiry. No-op when
        // auth-token-renewal is "on-demand" (default).
        scheduleProactiveRefreshIfEnabled(user, token);
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
    AgentConfig agentConfig = SecurityAuthAgent.getAgentConfig();
    boolean offline = agentConfig != null && agentConfig.isOfflineAccess();

    // Try refresh first if we have a refresh token
    if (existing != null && existing.getRefreshToken() != null) {
      try {
        OAuthToken refreshed = CLIENT.refreshToken(serverUrl,
            clientId, existing.getRefreshToken(), offline);
        replaceLoginUser(refreshed.getAccessToken());
        return refreshed;
      } catch (IOException e) {
        org.apache.ozone.AgentLog.warn(
            "Refresh failed, re-authenticating: " + e.getMessage());
      }
    }

    // Obtain new token with credentials
    if (provider.hasCredentials()) {
      OAuthToken token = CLIENT.obtainToken(serverUrl, clientId,
          provider.getLogin(), provider.getPassword(), offline);
      replaceLoginUser(token.getAccessToken());
      return token;
    }

    // Fall back to interactive flow if no credentials
    AgentConfig config = SecurityAuthAgent.getAgentConfig();
    if (config != null) {
      InteractiveAuthFlow flow =
          new InteractiveAuthFlow(config.isQrEnabled(), clientId,
              config.isOfflineAccess());
      OAuthToken token = flow.execute(serverUrl);
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
      AgentConfig config = SecurityAuthAgent.getAgentConfig();
      String principal = OAuthPrincipalBuilder.build(
          accessToken,
          config != null ? config.getKerberosHost() : null,
          config != null ? config.getKerberosRealm() : null);
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
      // If a Kerberos-shaped loginUser is already installed — either
      // by LoginInterceptor (cluster service principal) or by a
      // prior replaceLoginUser — don't overwrite it. obtainOrRefresh
      // calls this method on every token refresh; without this
      // guard, every DoAsInterceptor-triggered refresh clobbers the
      // LoginInterceptor's setLoginUser, sending the wrong principal
      // to service-to-service ACL checks (e.g. dn -> SCM secret-key
      // protocol expects dn/dn@EXAMPLE.COM, not dn/host@OZONE).
      if (isKerberosShapedLoginUser(resolvedUgiClass)) {
        return;
      }
      Class<?> authMethodClass = Class.forName(
          "org.apache.hadoop.security.SaslRpcServer$AuthMethod",
          true, cl);
      Object kerberos =
          Enum.valueOf((Class<Enum>) authMethodClass, "KERBEROS");

      java.lang.reflect.Method create = resolvedUgiClass.getMethod(
          "createRemoteUser", String.class, authMethodClass);
      Object ugi = create.invoke(null, principal, kerberos);

      java.lang.reflect.Method setLoginUser =
          resolvedUgiClass.getDeclaredMethod("setLoginUser",
              resolvedUgiClass);
      setLoginUser.setAccessible(true);
      setLoginUser.invoke(null, ugi);

      // Cache the OAuth UGI so the connection-context interceptor
      // can substitute it when an existing ticket holds the OS user.
      oauthUgi = ugi;

      org.apache.ozone.AgentLog.info(
          "Replaced login UGI with OAuth user: " + principal);
    } catch (Throwable e) {
      // Hadoop's KerberosName.rules is null until UGI has read
      // auth_to_local from core-site.xml. That doesn't happen until
      // Hadoop's own initialization runs, which is AFTER agent
      // premain. We get one shot here that fails on prewarmAuth and
      // a second, successful one when the first RPC triggers
      // DoAsInterceptor -> getToken -> replaceLoginUser. Don't
      // print a noisy stack for the expected first-call failure;
      // do print one for unexpected failures.
      Throwable cause = unwrap(e);
      if (cause instanceof NullPointerException
          && String.valueOf(cause.getMessage()).contains(
              "KerberosName.rules")) {
        // expected at premain — silent
        return;
      }
      org.apache.ozone.AgentLog.error(
          "Failed to replace login UGI: "
              + cause.getClass().getName() + ": " + cause.getMessage(),
          cause);
    }
  }

  private static Throwable unwrap(Throwable t) {
    Throwable cur = t;
    while (cur instanceof java.lang.reflect.InvocationTargetException
        && cur.getCause() != null) {
      cur = cur.getCause();
    }
    return cur;
  }

  /**
   * Returns true if the current login UGI's username looks like a
   * Kerberos principal ({@code service/host@REALM}). Used as a
   * guard against {@link #replaceLoginUser(String)} overwriting a
   * principal that was already installed by
   * {@link org.apache.ozone.interceptor.LoginInterceptor} (a service
   * keytab login) or a prior {@code replaceLoginUser} call.
   */
  private static boolean isKerberosShapedLoginUser(Class<?> ugiCls) {
    try {
      java.lang.reflect.Method getLoginUser =
          ugiCls.getMethod("getLoginUser");
      Object loginUgi = getLoginUser.invoke(null);
      if (loginUgi == null) {
        return false;
      }
      Object name = loginUgi.getClass().getMethod("getUserName")
          .invoke(loginUgi);
      if (!(name instanceof String)) {
        return false;
      }
      String s = (String) name;
      return s.indexOf('/') > 0 && s.indexOf('@') > 0;
    } catch (Throwable t) {
      return false;
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
          org.apache.ozone.AgentLog.info(
              "Loaded bundled OAuth token from UGI credentials for user "
                  + id.username);
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
      org.apache.ozone.AgentLog.info(
          "Bundled OAUTH-CREDS token into UGI credentials for user "
              + username);
    } catch (Throwable t) {
      org.apache.ozone.AgentLog.error(
          "Could not bundle OAuth token into UGI: " + t.getMessage(),
          t);
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

  // ---------------------------------------------------------------
  // Proactive token refresh — see AgentConfig.tokenRenewalMode.
  // ---------------------------------------------------------------

  private static boolean isProactiveRenewalEnabled() {
    AgentConfig config = SecurityAuthAgent.getAgentConfig();
    if (config == null) {
      return false;
    }
    String mode = config.getTokenRenewalMode();
    return "proactive".equals(mode) || "both".equals(mode);
  }

  private static ScheduledExecutorService refreshScheduler() {
    ScheduledExecutorService s = REFRESH_SCHEDULER;
    if (s != null) {
      return s;
    }
    synchronized (OAuthTokenManager.class) {
      s = REFRESH_SCHEDULER;
      if (s == null) {
        s = Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "SecurityAuthAgent-TokenRefresher");
          t.setDaemon(true);
          return t;
        });
        REFRESH_SCHEDULER = s;
      }
    }
    return s;
  }

  private static void scheduleProactiveRefreshIfEnabled(
      String user, OAuthToken token) {
    if (!isProactiveRenewalEnabled()) {
      return;
    }
    if (token == null || token.getRefreshToken() == null) {
      // Nothing to refresh proactively with — fall through to the
      // reactive obtain path next time getToken is called.
      return;
    }
    long delay = token.getExpiresAt() - System.currentTimeMillis()
        - PROACTIVE_REFRESH_BUFFER_MS;
    if (delay < PROACTIVE_REFRESH_MIN_DELAY_MS) {
      delay = PROACTIVE_REFRESH_MIN_DELAY_MS;
    }
    ScheduledFuture<?> next = refreshScheduler().schedule(
        () -> proactiveRefresh(user), delay, TimeUnit.MILLISECONDS);
    ScheduledFuture<?> prev = REFRESH_FUTURES.put(user, next);
    if (prev != null) {
      prev.cancel(false);
    }
  }

  private static void proactiveRefresh(String user) {
    try {
      OAuthToken cached = TOKEN_CACHE.get(user);
      if (cached == null) {
        return;
      }
      AuthDataProvider provider = SecurityAuthAgent.getProvider();
      if (provider == null) {
        return;
      }
      String serverUrl = provider.getServerUrl();
      if (serverUrl == null || serverUrl.isEmpty()) {
        return;
      }
      String clientId = provider.getClientId();
      AgentConfig config = SecurityAuthAgent.getAgentConfig();
      boolean offline = config != null && config.isOfflineAccess();

      OAuthToken refreshed = null;
      if (cached.getRefreshToken() != null) {
        try {
          refreshed = CLIENT.refreshToken(serverUrl, clientId,
              cached.getRefreshToken(), offline);
        } catch (IOException e) {
          org.apache.ozone.AgentLog.warn(
              "Proactive refresh failed for " + user
                  + ", re-authenticating: " + e.getMessage());
        }
      }
      if (refreshed == null && provider.hasCredentials()) {
        // Fallback: full re-auth with the env-provider credentials.
        refreshed = CLIENT.obtainToken(serverUrl, clientId,
            provider.getLogin(), provider.getPassword(), offline);
      }
      if (refreshed == null) {
        org.apache.ozone.AgentLog.warn(
            "Proactive refresh gave up for " + user
                + " — no refresh token and no provider credentials");
        return;
      }
      TOKEN_CACHE.put(user, refreshed);
      CURRENT_ACCESS_TOKEN.set(refreshed.getAccessToken());
      fallbackAccessToken = refreshed.getAccessToken();
      org.apache.ozone.AgentLog.debug(
          "Proactively refreshed OAuth token for: " + user);
      // Chain the next refresh.
      scheduleProactiveRefreshIfEnabled(user, refreshed);
    } catch (Throwable t) {
      // Never let an exception kill the scheduler thread.
      org.apache.ozone.AgentLog.warn(
          "Proactive refresh task crashed for " + user + ": "
              + t.getMessage());
    }
  }
}
