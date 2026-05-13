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
import org.apache.ozone.oauth.OAuthTokenManager;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Intercepts {@code KerberosAuthenticator.authenticate(URL, Token)}
 * to skip SPNEGO and instead authenticate via the OAuth Bearer token.
 *
 * <p>Hadoop services make HTTP calls to each other (e.g.,
 * Recon → OM for {@code /dbCheckpoint}) using
 * {@code AuthenticatedURL}. With Kerberos disabled, those calls
 * would otherwise hit the OIDC redirect endpoint of the server.
 * This interceptor opens the URL with {@code Authorization: Bearer},
 * the server's OIDC handler validates the token, returns an
 * {@code AuthenticationToken}, and {@code AuthenticationFilter}
 * sets the hadoop.auth session cookie. The cookie is extracted into
 * the {@code AuthenticatedURL.Token} so subsequent calls reuse it.
 */
public class KerberosAuthenticatorInterceptor {

  private static volatile Method extractTokenMethod;

  @RuntimeType
  public static void authenticate(@AllArguments Object[] args)
      throws IOException {
    URL url = (URL) args[0];
    Object token = args[1];

    String oauthToken = OAuthTokenManager.getCurrentAccessToken();
    if (oauthToken == null) {
      System.err.println(
          "[SecurityAuthAgent] No OAuth token available for HTTP "
              + "auth to " + url);
      return;
    }

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("HEAD");
      conn.setRequestProperty("Authorization", "Bearer " + oauthToken);
      conn.setConnectTimeout(10_000);
      conn.setReadTimeout(10_000);
      conn.connect();
      int status = conn.getResponseCode();
      System.out.println(
          "[SecurityAuthAgent] HTTP Bearer auth to " + url
              + " returned " + status);
      if (status == 200 || status == 405) {
        // Extract auth cookie set by AuthenticationFilter into the
        // AuthenticatedURL.Token so subsequent calls reuse it.
        try {
          getExtractTokenMethod(token).invoke(null, conn, token);
        } catch (Exception e) {
          System.err.println(
              "[SecurityAuthAgent] Failed to extract auth cookie: "
                  + e.getMessage());
        }
      } else if (status >= 400) {
        throw new IOException(
            "HTTP Bearer auth failed: " + status);
      }
    } finally {
      conn.disconnect();
    }
  }

  private static Method getExtractTokenMethod(Object token)
      throws Exception {
    if (extractTokenMethod == null) {
      synchronized (KerberosAuthenticatorInterceptor.class) {
        if (extractTokenMethod == null) {
          ClassLoader cl = token.getClass().getClassLoader();
          Class<?> authUrlClass = Class.forName(
              "org.apache.hadoop.security.authentication.client"
                  + ".AuthenticatedURL",
              true, cl);
          extractTokenMethod = authUrlClass.getMethod(
              "extractToken",
              HttpURLConnection.class, token.getClass());
        }
      }
    }
    return extractTokenMethod;
  }
}
