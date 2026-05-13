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
import org.apache.ozone.interceptor.SimpleConfigParser;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Reads OAuth credentials from a file specified via
 * {@code --auth-data-file-path}. Same format as
 * {@link DefaultAuthDataProvider}.
 */
public class FileAuthDataProvider implements AuthDataProvider {

  private String serverUrl;
  private String clientId;
  private String login;
  private String password;

  @Override
  public String getName() {
    return "file";
  }

  @Override
  public void init(AgentConfig config) {
    String filePath = config.getFilePath();
    if (filePath == null || filePath.isEmpty()) {
      System.err.println(
          "[SecurityAuthAgent] auth-data-file-path is required "
              + "for 'file' provider");
      return;
    }
    Path path = Paths.get(filePath);
    try {
      Map<String, Map<String, String>> sections =
          SimpleConfigParser.parse(path);
      serverUrl = SimpleConfigParser.getValue(sections, "auth",
          "server_url");
      clientId = SimpleConfigParser.getValue(sections, "auth",
          "client_id");
      login = SimpleConfigParser.getValue(sections, "cred", "login");
      password = SimpleConfigParser.getValue(sections, "cred", "password");
    } catch (Exception e) {
      System.err.println("[SecurityAuthAgent] Failed to read "
          + path + ": " + e.getMessage());
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
