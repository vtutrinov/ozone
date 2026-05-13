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

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

/**
 * Token validator that decodes the JWT payload without verifying the
 * signature. Extracts the username from {@code preferred_username}
 * or {@code sub} claim. Suitable for dev/test environments where
 * both sides trust the same OAuth server.
 */
public class NoneTokenValidator implements TokenValidator {

  @Override
  public String validate(String accessToken) throws IOException {
    if (accessToken == null || accessToken.isEmpty()) {
      throw new IOException("Empty access token");
    }
    String[] parts = accessToken.split("\\.");
    if (parts.length < 2) {
      throw new IOException("Invalid JWT format");
    }
    String payloadJson = new String(
        Base64.getUrlDecoder().decode(parts[1]), "UTF-8");
    Map<String, String> claims = SimpleJsonParser.parse(payloadJson);
    String username = claims.get("preferred_username");
    if (username == null) {
      username = claims.get("sub");
    }
    if (username == null) {
      throw new IOException(
          "No username found in JWT payload: " + payloadJson);
    }
    return username;
  }
}
