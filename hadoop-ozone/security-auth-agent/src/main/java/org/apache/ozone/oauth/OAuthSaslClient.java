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

import java.nio.charset.StandardCharsets;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * SASL client that sends the caller's Kerberos principal and OAuth
 * access token instead of a GSSAPI token. Reports mechanism as
 * "GSSAPI" to maintain protocol compatibility with the Hadoop RPC
 * SASL framing.
 *
 * <p>Wire format (newline-separated):
 * <pre>
 * &lt;full-kerberos-principal&gt;\n&lt;oauth-access-token&gt;
 * </pre>
 *
 * <p>The principal lets server-side authorization checks pass
 * (e.g. "this service is only accessible by om/om@EXAMPLE.COM").
 * The server verifies the JWT username matches the principal's
 * primary to prevent a client from spoofing a different identity.
 */
public class OAuthSaslClient implements SaslClient {

  static final String SEPARATOR = "\n";

  private final String accessToken;
  private final String principal;
  private boolean complete;

  public OAuthSaslClient(String accessToken, String principal) {
    this.accessToken = accessToken;
    this.principal = principal;
  }

  @Override
  public String getMechanismName() {
    return "GSSAPI";
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    complete = true;
    String payload = principal + SEPARATOR + accessToken;
    return payload.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len)
      throws SaslException {
    throw new SaslException("Unwrap not supported");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len)
      throws SaslException {
    throw new SaslException("Wrap not supported");
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    if ("javax.security.sasl.qop".equals(propName)) {
      return "auth";
    }
    return null;
  }

  @Override
  public void dispose() throws SaslException {
    // no-op
  }
}
