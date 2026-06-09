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
import org.apache.ozone.oauth.OAuthToken;
import org.apache.ozone.oauth.OAuthTokenManager;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Intercepts {@code UserGroupInformation.loginUserFromKeytab()} and
 * {@code loginUserFromKeytabAndReturnUGI()} to replace Kerberos login
 * with OAuth token acquisition.
 *
 * <p>Creates the UGI with {@code AuthMethod.KERBEROS} so that
 * downstream code (SASL negotiation, auth method checks) sees the
 * expected authentication method.
 */
public class LoginInterceptor {

  private static volatile Method createRemoteUserMethod;
  private static volatile Method setLoginUserMethod;
  private static volatile Object kerberosAuthMethod;
  private static volatile String cachedLocalHost;

  @RuntimeType
  public static Object intercept(@AllArguments Object[] args,
      @Origin Method method) throws Exception {
    String principal = substituteHostToken((String) args[0]);
    org.apache.ozone.AgentLog.debug("Intercepted " + method.getName()
        + " for principal: " + principal);

    // Obtain OAuth token instead of Kerberos ticket
    OAuthToken token = OAuthTokenManager.getToken(principal);
    if (token != null) {
      org.apache.ozone.AgentLog.info(
          "OAuth token obtained for: " + principal);
    }

    // Create a UGI with KERBEROS auth method via reflection.
    // Pass the FULL Kerberos principal so service authorization
    // checks (e.g., "this service is only accessible by
    // om/om@EXAMPLE.COM") pass. Hadoop's auth_to_local rules will
    // derive the short name when needed (e.g., for Ozone ACLs).
    Class<?> ugiClass = method.getDeclaringClass();
    ClassLoader cl = ugiClass.getClassLoader();
    Object ugi = getCreateRemoteUserMethod(ugiClass, cl)
        .invoke(null, principal, getKerberosAuthMethod(cl));

    // Always restore the cluster-configured service principal as
    // the loginUser, regardless of which keytab variant Hadoop
    // called. OAuthTokenManager.getToken(...) above triggered
    // replaceLoginUser, which set the loginUser to a JWT-derived
    // principal (e.g. om/host@OZONE). For service-to-service auth
    // the cluster's policy ACLs match the principal the cluster is
    // configured with (e.g. om/om@EXAMPLE.COM from
    // ozone.om.kerberos.principal), so we must overwrite the
    // JWT-shaped one with the cluster-shaped one here.
    getSetLoginUserMethod(ugiClass).invoke(null, ugi);

    if (method.getReturnType() == void.class) {
      return null;
    }
    // For loginUserFromKeytabAndReturnUGI, return the UGI as well.
    return ugi;
  }

  /**
   * Defensive substitution of Hadoop's {@code _HOST} placeholder in
   * principal strings. Hadoop's services normally resolve
   * {@code _HOST} via {@code SecurityUtil.getServerPrincipal} before
   * calling {@code loginUserFromKeytab}, so the principal arriving
   * here is already substituted. This catches the rare path where
   * raw config values flow straight to {@code loginUserFromKeytab}
   * (some third-party Hadoop code, custom services), which would
   * otherwise leave a literal {@code _HOST} in the UGI's username
   * and break SASL principal matching.
   *
   * <p>No-op for principals that don't contain {@code _HOST}, so it
   * stays free on the hot path.
   */
  static String substituteHostToken(String principal) {
    if (principal == null || !principal.contains("/_HOST@")) {
      return principal;
    }
    return principal.replace("_HOST", localCanonicalHost());
  }

  private static String localCanonicalHost() {
    String cached = cachedLocalHost;
    if (cached != null) {
      return cached;
    }
    try {
      cached = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      cached = "localhost";
    }
    cachedLocalHost = cached;
    return cached;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object getKerberosAuthMethod(ClassLoader cl)
      throws Exception {
    if (kerberosAuthMethod == null) {
      synchronized (LoginInterceptor.class) {
        if (kerberosAuthMethod == null) {
          Class<?> authMethodClass = loadAuthMethodClass(cl);
          kerberosAuthMethod =
              Enum.valueOf((Class<Enum>) authMethodClass, "KERBEROS");
        }
      }
    }
    return kerberosAuthMethod;
  }

  private static Method getCreateRemoteUserMethod(Class<?> ugiClass,
      ClassLoader cl) throws Exception {
    if (createRemoteUserMethod == null) {
      synchronized (LoginInterceptor.class) {
        if (createRemoteUserMethod == null) {
          Class<?> authMethodClass = loadAuthMethodClass(cl);
          createRemoteUserMethod = ugiClass.getMethod(
              "createRemoteUser", String.class, authMethodClass);
        }
      }
    }
    return createRemoteUserMethod;
  }

  private static Class<?> loadAuthMethodClass(ClassLoader cl)
      throws ClassNotFoundException {
    return Class.forName(
        "org.apache.hadoop.security.SaslRpcServer$AuthMethod",
        true, cl);
  }

  private static Method getSetLoginUserMethod(Class<?> ugiClass)
      throws NoSuchMethodException {
    if (setLoginUserMethod == null) {
      synchronized (LoginInterceptor.class) {
        if (setLoginUserMethod == null) {
          Method m = ugiClass.getDeclaredMethod("setLoginUser",
              ugiClass);
          m.setAccessible(true);
          setLoginUserMethod = m;
        }
      }
    }
    return setLoginUserMethod;
  }
}
