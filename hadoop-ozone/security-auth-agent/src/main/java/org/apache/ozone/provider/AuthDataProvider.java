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
 * SPI interface for providing OAuth credentials to the security agent.
 * Implementations are discovered via {@link java.util.ServiceLoader}.
 */
public interface AuthDataProvider {

  /** Short name used in {@code auth-data-provider=<name>}. */
  String getName();

  /** Initialize with parsed agent config. */
  void init(AgentConfig config);

  /** OAuth token endpoint URL. */
  String getServerUrl();

  /** Username for password grant. May return null for interactive flow. */
  String getLogin();

  /** Password for password grant. May return null for interactive flow. */
  String getPassword();

  /** OAuth client_id (e.g. "ozone-client"). May return null. */
  String getClientId();

  /** Returns true if login and password are available. */
  boolean hasCredentials();
}
