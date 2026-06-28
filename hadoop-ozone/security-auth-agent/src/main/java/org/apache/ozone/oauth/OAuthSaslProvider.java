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

import java.security.Provider;

/**
 * JCA {@link Provider} that registers
 * {@link OAuthSaslClientFactory} and {@link OAuthSaslServerFactory}
 * for the {@code GSSAPI} mechanism. The agent calls
 * {@code Security.insertProviderAt(new OAuthSaslProvider(), 1)} in
 * premain so the OAuth factories are tried before the JDK's default
 * GSSAPI provider — Hive's thrift {@code TSaslClientTransport} (and
 * any other caller of {@link javax.security.sasl.Sasl#createSaslClient
 * Sasl.createSaslClient}) gets our OAuth flow without needing
 * additional bytecode interception.
 */
public final class OAuthSaslProvider extends Provider {

  private static final long serialVersionUID = 1L;
  public static final String NAME = "OZONE_OAUTH";

  public OAuthSaslProvider() {
    super(NAME, 1.0,
        "Ozone OAuth SASL provider — substitutes OAuth tokens for "
            + "Kerberos GSSAPI in client/server SASL handshakes.");
    put("SaslClientFactory.GSSAPI",
        OAuthSaslClientFactory.class.getName());
    put("SaslServerFactory.GSSAPI",
        OAuthSaslServerFactory.class.getName());
  }
}
