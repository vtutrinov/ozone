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

/**
 * Holds an OAuth access token, optional refresh token, and expiry.
 */
public class OAuthToken {

  private static final long EXPIRY_BUFFER_MS = 30_000;

  private final String accessToken;
  private final String refreshToken;
  private final long expiresAt;

  public OAuthToken(String accessToken, String refreshToken,
      long expiresAt) {
    this.accessToken = accessToken;
    this.refreshToken = refreshToken;
    this.expiresAt = expiresAt;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public boolean isExpired() {
    return System.currentTimeMillis() + EXPIRY_BUFFER_MS >= expiresAt;
  }

  @Override
  public String toString() {
    return "OAuthToken{expiresAt=" + expiresAt
        + ", expired=" + isExpired() + '}';
  }
}
