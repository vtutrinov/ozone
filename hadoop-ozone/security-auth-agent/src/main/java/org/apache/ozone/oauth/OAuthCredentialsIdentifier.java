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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop {@link TokenIdentifier} carrying an OAuth user identity.
 * Used to propagate the submitter's OAuth context with a YARN job so
 * the AM/task containers can authenticate to services on behalf of
 * the user.
 *
 * <p>The identifier holds metadata (username, OAuth server URL,
 * client ID, expiry). The {@code Token<?>}'s password bytes hold
 * the actual access/refresh tokens — opaque to Hadoop, populated
 * and read by the agent.
 */
public class OAuthCredentialsIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("OAUTH-CREDS");

  private String username = "";
  private String serverUrl = "";
  private String clientId = "";
  private long expiresAt;

  public OAuthCredentialsIdentifier() {
    // Required for ServiceLoader / readFields
  }

  public OAuthCredentialsIdentifier(String username, String serverUrl,
      String clientId, long expiresAt) {
    this.username = username == null ? "" : username;
    this.serverUrl = serverUrl == null ? "" : serverUrl;
    this.clientId = clientId == null ? "" : clientId;
    this.expiresAt = expiresAt;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (username == null || username.isEmpty()) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(username);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = OAuthCredentialsCodec.encodeIdentifier(
        username, serverUrl, clientId, expiresAt);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    OAuthCredentialsCodec.IdentifierFields f =
        OAuthCredentialsCodec.decodeIdentifier(bytes);
    this.username = f.username;
    this.serverUrl = f.serverUrl;
    this.clientId = f.clientId;
    this.expiresAt = f.expiresAt;
  }

  public String getUsername() {
    return username;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  public String getClientId() {
    return clientId;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public void setExpiresAt(long expiresAt) {
    this.expiresAt = expiresAt;
  }
}
