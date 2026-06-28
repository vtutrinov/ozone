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

import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

/**
 * JCA {@link SaslClientFactory} that returns an OAuth-backed
 * {@link OAuthSaslClient} whenever a caller asks for a {@code GSSAPI}
 * SASL client. Registered as the first provider for
 * {@code SaslClientFactory.GSSAPI} via {@link OAuthSaslProvider} so
 * Hive thrift's {@code TSaslClientTransport} (which calls
 * {@code Sasl.createSaslClient(new String[]{"GSSAPI"}, ...)}) picks
 * us up instead of the JDK default GSSAPI factory.
 *
 * <p>When the agent doesn't yet have an access token (e.g. before
 * pre-warm completes), {@link #createSaslClient} returns
 * {@code null} so the default GSSAPI factory is tried — important
 * for the few classpath users that legitimately want Kerberos
 * GSSAPI (none in our OAuth-only suites, but defensive anyway).
 */
public final class OAuthSaslClientFactory implements SaslClientFactory {

  private static final String[] MECHS = {"GSSAPI"};

  @Override
  public SaslClient createSaslClient(String[] mechanisms,
      String authorizationId, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) throws SaslException {
    if (!offersGssapi(mechanisms)) {
      return null;
    }
    String token = OAuthTokenManager.getCurrentAccessToken();
    if (token == null) {
      // No OAuth state yet — let the default JDK factory handle it.
      return null;
    }
    String principal = currentPrincipal(authorizationId, protocol,
        serverName);
    return new OAuthSaslClient(token, principal);
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return MECHS.clone();
  }

  private static boolean offersGssapi(String[] mechanisms) {
    if (mechanisms == null) {
      return false;
    }
    for (String m : mechanisms) {
      if ("GSSAPI".equals(m)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resolve which principal to claim on the wire. Prefer the
   * caller-supplied {@code authorizationId} (Hive thrift passes
   * {@code null}; some callers pass an explicit one). Then fall back
   * to the current UGI's login user via reflection so we don't have
   * to depend on hadoop-common at compile time. Last resort: the
   * SASL {@code protocol/serverName} form, since that's what a
   * Kerberos GSSAPI client would normally send.
   */
  private static String currentPrincipal(String authorizationId,
      String protocol, String serverName) {
    if (authorizationId != null && !authorizationId.isEmpty()) {
      return authorizationId;
    }
    String ugi = currentUgiUserName();
    if (ugi != null && !ugi.isEmpty()) {
      return ugi;
    }
    String svc = protocol != null ? protocol : "service";
    String host = serverName != null ? serverName : "localhost";
    return svc + "/" + host;
  }

  private static String currentUgiUserName() {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = OAuthSaslClientFactory.class.getClassLoader();
      }
      Class<?> ugiClass = Class.forName(
          "org.apache.hadoop.security.UserGroupInformation", true, cl);
      Object ugi = ugiClass.getMethod("getCurrentUser").invoke(null);
      if (ugi == null) {
        return null;
      }
      return (String) ugiClass.getMethod("getUserName").invoke(ugi);
    } catch (Throwable t) {
      return null;
    }
  }
}
