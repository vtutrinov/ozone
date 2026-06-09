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

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.ozone.SecurityAuthAgent;
import org.apache.ozone.config.AgentConfig;
import org.apache.ozone.oauth.IntrospectTokenValidator;
import org.apache.ozone.oauth.JwtTokenValidator;
import org.apache.ozone.oauth.NoneTokenValidator;
import org.apache.ozone.oauth.OAuthSaslServer;
import org.apache.ozone.oauth.TokenValidator;
import org.apache.ozone.provider.AuthDataProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import javax.security.sasl.SaslServer;

/**
 * Intercepts {@code SaslRpcServer.create()} to return an
 * {@link OAuthSaslServer} when KERBEROS auth is requested.
 * The token validation mode is determined by the agent config.
 */
public class SaslServerInterceptor {

  @RuntimeType
  public static SaslServer intercept(
      @This Object thiz,
      @Origin Method method,
      @SuperCall Callable<SaslServer> zuper) throws Exception {

    // Extract authMethod from the SaslRpcServer instance
    String authMethodName = getAuthMethod(thiz);

    if ("KERBEROS".equals(authMethodName)) {
      TokenValidator validator = createValidator();
      org.apache.ozone.AgentLog.debug(
          "Using OAuth SASL server instead of GSSAPI (validation: "
              + SecurityAuthAgent.getAgentConfig()
                  .getTokenValidation() + ")");
      return new OAuthSaslServer(validator);
    }

    // Non-KERBEROS — use original (e.g. TOKEN auth)
    return zuper.call();
  }

  private static String getAuthMethod(Object saslRpcServer) {
    try {
      // SaslRpcServer has an 'authMethod' field
      Field f = saslRpcServer.getClass().getDeclaredField("authMethod");
      f.setAccessible(true);
      Object authMethod = f.get(saslRpcServer);
      return authMethod != null ? authMethod.toString() : null;
    } catch (Exception e) {
      // Fallback: try name() if it's an enum
      try {
        Method m = saslRpcServer.getClass().getMethod("getAuthMethod");
        Object result = m.invoke(saslRpcServer);
        return result != null ? result.toString() : null;
      } catch (Exception e2) {
        return null;
      }
    }
  }

  private static TokenValidator createValidator() {
    AgentConfig config = SecurityAuthAgent.getAgentConfig();
    AuthDataProvider provider = SecurityAuthAgent.getProvider();
    String mode = config != null
        ? config.getTokenValidation() : "none";

    switch (mode) {
    case "introspect":
      String tokenUrl = provider != null
          ? provider.getServerUrl() : null;
      String clientId = provider != null
          ? provider.getClientId() : null;
      if (tokenUrl == null || clientId == null) {
        org.apache.ozone.AgentLog.warn(
            "introspect mode requires server URL and client ID, "
                + "falling back to none");
        return new NoneTokenValidator();
      }
      return new IntrospectTokenValidator(tokenUrl, clientId);
    case "jwt":
      String jwtTokenUrl = provider != null
          ? provider.getServerUrl() : null;
      if (jwtTokenUrl == null) {
        org.apache.ozone.AgentLog.warn(
            "jwt mode requires server URL, falling back to none");
        return new NoneTokenValidator();
      }
      return new JwtTokenValidator(jwtTokenUrl);
    case "none":
    default:
      return new NoneTokenValidator();
    }
  }
}
