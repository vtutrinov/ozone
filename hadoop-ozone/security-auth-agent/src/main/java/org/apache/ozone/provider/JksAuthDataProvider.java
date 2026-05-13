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
package org.apache.ozone.provider;

import org.apache.ozone.config.AgentConfig;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import javax.crypto.SecretKey;

/**
 * Reads OAuth credentials from a JKS keystore.
 *
 * <p>The keystore entry at the configured alias is expected to be a
 * {@link SecretKey} whose bytes encode a simple properties string:
 * <pre>
 * server_url=https://...
 * login=user
 * password=secret
 * </pre>
 */
public class JksAuthDataProvider implements AuthDataProvider {

  private String serverUrl;
  private String clientId;
  private String login;
  private String password;

  @Override
  public String getName() {
    return "jks";
  }

  @Override
  public void init(AgentConfig config) {
    String jksPath = config.getJksPath();
    String jksPassword = config.getJksPassword();
    String jksAlias = config.getJksAlias();

    if (jksPath == null || jksPassword == null || jksAlias == null) {
      System.err.println(
          "[SecurityAuthAgent] --auth-data-jks-path, "
              + "--auth-data-jks-password, and --auth-data-jks-alias "
              + "are all required for 'jks' provider");
      return;
    }

    try (InputStream is = Files.newInputStream(Paths.get(jksPath))) {
      KeyStore ks = KeyStore.getInstance("JCEKS");
      char[] pw = jksPassword.toCharArray();
      ks.load(is, pw);

      KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry)
          ks.getEntry(jksAlias,
              new KeyStore.PasswordProtection(pw));
      if (entry == null) {
        System.err.println(
            "[SecurityAuthAgent] No entry found for alias: " + jksAlias);
        return;
      }
      String data = new String(entry.getSecretKey().getEncoded(),
          "UTF-8");
      for (String line : data.split("\n")) {
        line = line.trim();
        int eq = line.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String key = line.substring(0, eq).trim();
        String value = line.substring(eq + 1).trim();
        switch (key) {
        case "server_url":
          serverUrl = value;
          break;
        case "client_id":
          clientId = value;
          break;
        case "login":
          login = value;
          break;
        case "password":
          this.password = value;
          break;
        default:
          break;
        }
      }
    } catch (Exception e) {
      System.err.println("[SecurityAuthAgent] Failed to read JKS: "
          + e.getMessage());
    }
  }

  @Override
  public String getServerUrl() {
    return serverUrl;
  }

  @Override
  public String getLogin() {
    return login;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public boolean hasCredentials() {
    return login != null && !login.isEmpty()
        && password != null && !password.isEmpty();
  }
}
