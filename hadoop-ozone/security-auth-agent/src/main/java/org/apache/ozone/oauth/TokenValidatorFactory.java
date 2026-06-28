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
import org.apache.ozone.provider.AuthDataProvider;

/**
 * Builds a {@link TokenValidator} matching the agent's
 * {@code auth-token-validation} arg. Shared by every code path
 * that needs to construct a server-side validator —
 * {@link OAuthSaslServer}, the SASL provider factory, and any
 * future direct-thrift interceptor.
 */
public final class TokenValidatorFactory {

  private TokenValidatorFactory() {
  }

  public static TokenValidator create() {
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
