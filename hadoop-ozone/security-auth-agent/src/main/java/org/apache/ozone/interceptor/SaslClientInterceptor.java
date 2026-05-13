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
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.ozone.oauth.NoneTokenValidator;
import org.apache.ozone.oauth.OAuthSaslClient;
import org.apache.ozone.oauth.OAuthTokenManager;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import javax.security.sasl.SaslClient;

/**
 * Intercepts {@code SaslRpcClient.createSaslClient(SaslAuth)} to
 * return an {@link OAuthSaslClient} when KERBEROS auth is requested.
 * This bypasses GSSAPI and sends the OAuth token instead.
 */
public class SaslClientInterceptor {

  @RuntimeType
  public static SaslClient intercept(
      @This Object thiz,
      @AllArguments Object[] args,
      @Origin Method method,
      @SuperCall Callable<SaslClient> zuper) throws Exception {

    // args[0] is SaslAuth — extract the auth method name
    Object saslAuth = args[0];
    String authMethod = (String) saslAuth.getClass()
        .getMethod("getMethod").invoke(saslAuth);

    if ("KERBEROS".equals(authMethod)) {
      String token = OAuthTokenManager.getCurrentAccessToken();
      if (token != null) {
        String ugiPrincipal = extractPrincipal(thiz);
        String principal = resolvePrincipal(ugiPrincipal, token);
        System.out.println(
            "[SecurityAuthAgent] Using OAuth SASL client "
                + "instead of GSSAPI (principal: " + principal + ")");
        return new OAuthSaslClient(token, principal);
      }
    }

    // Non-KERBEROS or no token — use original
    return zuper.call();
  }

  /**
   * Decide which principal to send in SASL.
   *
   * <p>If the UGI has a Kerberos-style principal (contains '/' or
   * '@'), use it — that's a service account (e.g., from
   * {@code LoginInterceptor}). Otherwise the UGI is the OS user
   * (e.g., "hadoop" in a CLI container) and we should use the
   * OAuth-authenticated identity from the JWT instead.
   */
  private static String resolvePrincipal(String ugiPrincipal,
      String accessToken) {
    if (ugiPrincipal != null
        && (ugiPrincipal.indexOf('/') > 0
            || ugiPrincipal.indexOf('@') > 0)) {
      return ugiPrincipal;
    }
    try {
      String jwtUser = new NoneTokenValidator().validate(accessToken);
      if (jwtUser != null && !jwtUser.isEmpty()) {
        return jwtUser;
      }
    } catch (Exception e) {
      System.err.println(
          "[SecurityAuthAgent] Could not extract JWT username: "
              + e.getMessage());
    }
    return ugiPrincipal != null ? ugiPrincipal : "unknown";
  }

  private static String extractPrincipal(Object saslRpcClient) {
    try {
      Field ugiField =
          saslRpcClient.getClass().getDeclaredField("ugi");
      ugiField.setAccessible(true);
      Object ugi = ugiField.get(saslRpcClient);
      if (ugi != null) {
        return (String) ugi.getClass()
            .getMethod("getUserName").invoke(ugi);
      }
    } catch (Exception e) {
      System.err.println(
          "[SecurityAuthAgent] Could not extract principal: "
              + e.getMessage());
    }
    return "unknown";
  }
}
