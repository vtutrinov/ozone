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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Renews an {@link OAuthCredentialsIdentifier}-backed token by using
 * the stored refresh_token to obtain a new access_token from the
 * OAuth server.
 *
 * <p>Registered via {@code META-INF/services/
 * org.apache.hadoop.security.token.TokenRenewer}.
 *
 * <p>Hadoop's YARN {@code DelegationTokenRenewer} discovers this
 * renewer through the SPI and calls {@link #renew} periodically for
 * the duration of the YARN application.
 */
public class OAuthTokenRenewer extends TokenRenewer {

  @Override
  public boolean handleKind(Text kind) {
    return OAuthCredentialsIdentifier.KIND_NAME.equals(kind);
  }

  @Override
  public boolean isManaged(Token<?> token) {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException {
    OAuthCredentialsIdentifier id = decodeIdentifier(token);
    OAuthCredentialsCodec.PasswordFields pw =
        OAuthCredentialsCodec.decodePassword(token.getPassword());

    if (pw.refreshToken == null || pw.refreshToken.isEmpty()) {
      throw new IOException(
          "Cannot renew OAuth token: no refresh_token in password bytes");
    }

    org.apache.ozone.AgentLog.info(
        "Renewing OAuth token for user " + id.getUsername());

    OAuthClient client = new OAuthClient();
    OAuthToken refreshed = client.refreshToken(
        id.getServerUrl(), id.getClientId(), pw.refreshToken);

    // Update token in place: identifier (expiresAt) + password bytes
    id.setExpiresAt(refreshed.getExpiresAt());
    byte[] newIdBytes = OAuthCredentialsCodec.encodeIdentifier(
        id.getUsername(), id.getServerUrl(), id.getClientId(),
        refreshed.getExpiresAt());
    byte[] newPwBytes = OAuthCredentialsCodec.encodePassword(
        refreshed.getAccessToken(),
        refreshed.getRefreshToken() != null
            ? refreshed.getRefreshToken() : pw.refreshToken);

    // The Token class doesn't expose setters for identifier/password,
    // but mutating its byte[] fields via reflection is the standard
    // pattern (matches what Hadoop's own renewers effectively do
    // because Hadoop renewers usually generate a *new* token rather
    // than mutating — but for OAuth we want in-place update so YARN's
    // existing reference stays valid).
    setField(token, "identifier", newIdBytes);
    setField(token, "password", newPwBytes);

    return refreshed.getExpiresAt();
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) {
    // No-op: OAuth tokens expire naturally. If/when Keycloak
    // session-logout via refresh_token is desired, hook here.
    org.apache.ozone.AgentLog.info(
        "OAuth token cancel requested — ignored");
  }

  private static OAuthCredentialsIdentifier decodeIdentifier(Token<?> token)
      throws IOException {
    TokenIdentifier raw = token.decodeIdentifier();
    if (raw instanceof OAuthCredentialsIdentifier) {
      return (OAuthCredentialsIdentifier) raw;
    }
    // Fallback: SPI lookup may have failed; deserialize manually.
    OAuthCredentialsIdentifier id = new OAuthCredentialsIdentifier();
    java.io.ByteArrayInputStream bis =
        new java.io.ByteArrayInputStream(token.getIdentifier());
    id.readFields(new java.io.DataInputStream(bis));
    return id;
  }

  private static void setField(Object target, String fieldName,
      byte[] value) throws IOException {
    try {
      Field f = Token.class.getDeclaredField(fieldName);
      f.setAccessible(true);
      f.set(target, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IOException(
          "Could not update Token." + fieldName + " in place", e);
    }
  }
}
