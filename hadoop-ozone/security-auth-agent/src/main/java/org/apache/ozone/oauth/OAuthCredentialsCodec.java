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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Encode/decode the payload of {@link OAuthCredentialsIdentifier}
 * and the Hadoop {@code Token<?>} password bytes.
 *
 * <p>Identifier layout (visible/signed):
 * <pre>
 *   UTF: username
 *   UTF: serverUrl
 *   UTF: clientId
 *   long: expiresAt (epoch millis)
 * </pre>
 *
 * <p>Password layout (opaque/secret):
 * <pre>
 *   UTF: accessToken
 *   UTF: refreshToken (may be empty string)
 * </pre>
 */
public final class OAuthCredentialsCodec {

  private OAuthCredentialsCodec() {
  }

  public static byte[] encodeIdentifier(String username, String serverUrl,
      String clientId, long expiresAt) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeUTF(nullSafe(username));
      dos.writeUTF(nullSafe(serverUrl));
      dos.writeUTF(nullSafe(clientId));
      dos.writeLong(expiresAt);
    }
    return bos.toByteArray();
  }

  public static IdentifierFields decodeIdentifier(byte[] bytes)
      throws IOException {
    try (DataInputStream dis =
             new DataInputStream(new ByteArrayInputStream(bytes))) {
      IdentifierFields f = new IdentifierFields();
      f.username = dis.readUTF();
      f.serverUrl = dis.readUTF();
      f.clientId = dis.readUTF();
      f.expiresAt = dis.readLong();
      return f;
    }
  }

  public static byte[] encodePassword(String accessToken,
      String refreshToken) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeUTF(nullSafe(accessToken));
      dos.writeUTF(nullSafe(refreshToken));
    }
    return bos.toByteArray();
  }

  public static PasswordFields decodePassword(byte[] bytes)
      throws IOException {
    try (DataInputStream dis =
             new DataInputStream(new ByteArrayInputStream(bytes))) {
      PasswordFields f = new PasswordFields();
      f.accessToken = dis.readUTF();
      f.refreshToken = dis.readUTF();
      return f;
    }
  }

  /**
   * Encode identifier in the form stored as {@code Token.identifier}
   * — i.e., length-prefixed, matching what
   * {@link OAuthCredentialsIdentifier#readFields(java.io.DataInput)}
   * expects.
   */
  public static byte[] encodeIdentifierForTokenForm(String username,
      String serverUrl, String clientId, long expiresAt)
      throws IOException {
    byte[] payload = encodeIdentifier(
        username, serverUrl, clientId, expiresAt);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeInt(payload.length);
      dos.write(payload);
    }
    return bos.toByteArray();
  }

  /**
   * Inverse of {@link #encodeIdentifierForTokenForm}. Reads a
   * length-prefixed identifier blob (the format
   * {@link OAuthCredentialsIdentifier#write} produces) and returns
   * the decoded fields.
   */
  public static IdentifierFields decodeIdentifierFromTokenForm(
      byte[] bytes) throws IOException {
    try (DataInputStream dis =
             new DataInputStream(new ByteArrayInputStream(bytes))) {
      int len = dis.readInt();
      byte[] payload = new byte[len];
      dis.readFully(payload);
      return decodeIdentifier(payload);
    }
  }

  private static String nullSafe(String s) {
    return s == null ? "" : s;
  }

  /** Decoded identifier fields. */
  public static class IdentifierFields {
    public String username;
    public String serverUrl;
    public String clientId;
    public long expiresAt;
  }

  /** Decoded password fields. */
  public static class PasswordFields {
    public String accessToken;
    public String refreshToken;
  }
}
