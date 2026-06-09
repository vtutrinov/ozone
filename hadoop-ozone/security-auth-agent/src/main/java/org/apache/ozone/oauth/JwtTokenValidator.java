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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates JWT tokens locally by verifying the RSA signature
 * against public keys fetched from the OAuth server's JWKS endpoint.
 * Keys are cached and refreshed periodically.
 */
public class JwtTokenValidator implements TokenValidator {

  private static final int TIMEOUT_MS = 10_000;
  private static final long JWKS_CACHE_TTL_MS = 300_000; // 5 min

  private final String jwksUrl;
  private final ConcurrentHashMap<String, PublicKey> keyCache =
      new ConcurrentHashMap<>();
  private volatile long keysCachedAt;

  /**
   * @param tokenUrl the OAuth token endpoint URL
   */
  public JwtTokenValidator(String tokenUrl) {
    // Keycloak: /protocol/openid-connect/token →
    //           /protocol/openid-connect/certs
    if (tokenUrl.endsWith("/token")) {
      this.jwksUrl = tokenUrl.substring(0,
          tokenUrl.length() - "/token".length()) + "/certs";
    } else {
      this.jwksUrl = tokenUrl + "/certs";
    }
  }

  @Override
  public String validate(String accessToken) throws IOException {
    if (accessToken == null || accessToken.isEmpty()) {
      throw new IOException("Empty access token");
    }
    String[] parts = accessToken.split("\\.");
    if (parts.length != 3) {
      throw new IOException("Invalid JWT format: expected 3 parts");
    }

    // Decode header to get key ID
    String headerJson = new String(
        Base64.getUrlDecoder().decode(parts[0]), "UTF-8");
    Map<String, String> header = SimpleJsonParser.parse(headerJson);
    String kid = header.get("kid");
    String alg = header.get("alg");

    if (alg == null || !alg.startsWith("RS")) {
      throw new IOException("Unsupported JWT algorithm: " + alg);
    }

    // Get public key
    PublicKey key = getPublicKey(kid);
    if (key == null) {
      // Refresh keys and retry
      refreshKeys();
      key = getPublicKey(kid);
    }
    if (key == null) {
      throw new IOException("No public key found for kid: " + kid);
    }

    // Verify signature
    verifySignature(parts[0], parts[1], parts[2], alg, key);

    // Decode payload and extract username
    String payloadJson = new String(
        Base64.getUrlDecoder().decode(parts[1]), "UTF-8");
    Map<String, String> claims = SimpleJsonParser.parse(payloadJson);

    // Check expiry
    String expStr = claims.get("exp");
    if (expStr != null) {
      try {
        long exp = Long.parseLong(expStr);
        if (System.currentTimeMillis() / 1000 > exp) {
          throw new IOException("JWT token expired");
        }
      } catch (NumberFormatException e) {
        // ignore
      }
    }

    String username = claims.get("preferred_username");
    if (username == null) {
      username = claims.get("sub");
    }
    if (username == null) {
      throw new IOException("No username in JWT claims");
    }
    return username;
  }

  private void verifySignature(String headerB64, String payloadB64,
      String signatureB64, String alg, PublicKey key)
      throws IOException {
    try {
      String javaAlg;
      switch (alg) {
      case "RS256":
        javaAlg = "SHA256withRSA";
        break;
      case "RS384":
        javaAlg = "SHA384withRSA";
        break;
      case "RS512":
        javaAlg = "SHA512withRSA";
        break;
      default:
        throw new IOException("Unsupported algorithm: " + alg);
      }

      byte[] signedData = (headerB64 + "." + payloadB64)
          .getBytes(StandardCharsets.UTF_8);
      byte[] signature = Base64.getUrlDecoder().decode(signatureB64);

      Signature sig = Signature.getInstance(javaAlg);
      sig.initVerify(key);
      sig.update(signedData);
      if (!sig.verify(signature)) {
        throw new IOException("JWT signature verification failed");
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("JWT signature verification error", e);
    }
  }

  private PublicKey getPublicKey(String kid) throws IOException {
    if (shouldRefreshKeys()) {
      refreshKeys();
    }
    if (kid != null) {
      return keyCache.get(kid);
    }
    // If no kid specified, return the first key
    return keyCache.isEmpty() ? null : keyCache.values().iterator().next();
  }

  private boolean shouldRefreshKeys() {
    return keyCache.isEmpty()
        || System.currentTimeMillis() - keysCachedAt > JWKS_CACHE_TTL_MS;
  }

  private synchronized void refreshKeys() throws IOException {
    if (!shouldRefreshKeys()) {
      return; // another thread refreshed
    }
    String jwksJson = fetchJwks();
    parseAndCacheKeys(jwksJson);
    keysCachedAt = System.currentTimeMillis();
  }

  private String fetchJwks() throws IOException {
    URL url = new URL(jwksUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("Accept", "application/json");

      int status = conn.getResponseCode();
      if (status != 200) {
        throw new IOException(
            "JWKS fetch failed (HTTP " + status + ")");
      }
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(),
              StandardCharsets.UTF_8))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        return sb.toString();
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Parse JWKS response and extract RSA public keys.
   * JWKS format: {"keys":[{"kty":"RSA","kid":"...","n":"...","e":"..."}]}
   * We use simple string parsing since the structure is predictable.
   */
  private void parseAndCacheKeys(String jwksJson) throws IOException {
    keyCache.clear();
    // Split by key objects — find each {"kty" block
    int idx = 0;
    while (true) {
      int start = jwksJson.indexOf("{\"", idx);
      if (start < 0) {
        break;
      }
      int end = findMatchingBrace(jwksJson, start);
      if (end < 0) {
        break;
      }
      String keyJson = jwksJson.substring(start, end + 1);
      Map<String, String> keyFields = SimpleJsonParser.parse(keyJson);
      if ("RSA".equals(keyFields.get("kty"))) {
        String kid = keyFields.get("kid");
        String n = keyFields.get("n");
        String e = keyFields.get("e");
        if (n != null && e != null) {
          try {
            PublicKey pk = buildRsaPublicKey(n, e);
            keyCache.put(kid != null ? kid : "default", pk);
          } catch (Exception ex) {
            org.apache.ozone.AgentLog.warn(
                "Failed to parse RSA key " + kid + ": "
                    + ex.getMessage());
          }
        }
      }
      idx = end + 1;
    }
    if (keyCache.isEmpty()) {
      throw new IOException("No RSA keys found in JWKS response");
    }
    org.apache.ozone.AgentLog.info(
        "Cached " + keyCache.size() + " JWKS public key(s)");
  }

  private static int findMatchingBrace(String s, int start) {
    int depth = 0;
    boolean inString = false;
    for (int i = start; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\') {
        i++;
        continue;
      }
      if (c == '"') {
        inString = !inString;
      } else if (!inString) {
        if (c == '{') {
          depth++;
        } else if (c == '}') {
          depth--;
          if (depth == 0) {
            return i;
          }
        }
      }
    }
    return -1;
  }

  private static PublicKey buildRsaPublicKey(String nBase64,
      String eBase64) throws Exception {
    byte[] nBytes = Base64.getUrlDecoder().decode(nBase64);
    byte[] eBytes = Base64.getUrlDecoder().decode(eBase64);
    BigInteger modulus = new BigInteger(1, nBytes);
    BigInteger exponent = new BigInteger(1, eBytes);
    RSAPublicKeySpec spec = new RSAPublicKeySpec(modulus, exponent);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return kf.generatePublic(spec);
  }
}
