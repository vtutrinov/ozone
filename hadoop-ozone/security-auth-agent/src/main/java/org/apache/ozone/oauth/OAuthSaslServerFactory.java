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
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * JCA {@link SaslServerFactory} that returns an
 * {@link OAuthSaslServer} for the {@code GSSAPI} mechanism. The
 * server validates the JWT carried in the SASL initial response
 * with the {@link TokenValidator} chosen by the agent's
 * {@code auth-token-validation} arg ({@code none} / {@code jwt} /
 * {@code introspect}).
 *
 * <p>Returns {@code null} for any other mechanism so the JDK
 * defaults can handle DIGEST-MD5 / PLAIN / etc.
 */
public final class OAuthSaslServerFactory implements SaslServerFactory {

  private static final String[] MECHS = {"GSSAPI"};

  @Override
  public SaslServer createSaslServer(String mechanism, String protocol,
      String serverName, Map<String, ?> props, CallbackHandler cbh)
      throws SaslException {
    if (!"GSSAPI".equals(mechanism)) {
      return null;
    }
    return new OAuthSaslServer(TokenValidatorFactory.create());
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return MECHS.clone();
  }
}
