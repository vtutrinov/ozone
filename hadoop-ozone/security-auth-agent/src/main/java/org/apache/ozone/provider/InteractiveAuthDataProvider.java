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

/**
 * Auth data provider for interactive (CLI) usage. Reads OAuth
 * server URL and client ID from environment, but does NOT read
 * login/password — instead, {@link OAuthTokenManager} falls back
 * to {@link InteractiveAuthFlow} (Device Authorization Grant)
 * which prints a login URL+code for the user.
 *
 * <p>Activate with agent arg {@code auth-data-provider=interactive}.
 */
public class InteractiveAuthDataProvider implements AuthDataProvider {

  private String serverUrl;
  private String clientId;

  @Override
  public String getName() {
    return "interactive";
  }

  @Override
  public void init(AgentConfig config) {
    serverUrl = System.getenv("AUTH_SERVER_URL");
    clientId = System.getenv("AUTH_CLIENT_ID");
  }

  @Override
  public String getServerUrl() {
    return serverUrl;
  }

  @Override
  public String getLogin() {
    return null;
  }

  @Override
  public String getPassword() {
    return null;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public boolean hasCredentials() {
    return false;
  }
}
