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
package org.apache.ozone.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAgentArgsParser {

  @Test
  void nullArgs() {
    AgentConfig config = AgentArgsParser.parse(null);
    assertEquals("default", config.getProviderName());
    assertNull(config.getFilePath());
    assertFalse(config.isQrEnabled());
  }

  @Test
  void emptyArgs() {
    AgentConfig config = AgentArgsParser.parse("");
    assertEquals("default", config.getProviderName());
  }

  @Test
  void singleProvider() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env");
    assertEquals("env", config.getProviderName());
  }

  @Test
  void multipleArgs() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=file,"
            + "auth-data-file-path=/tmp/auth.conf,"
            + "auth-qr=true");
    assertEquals("file", config.getProviderName());
    assertEquals("/tmp/auth.conf", config.getFilePath());
    assertTrue(config.isQrEnabled());
  }

  @Test
  void jksArgs() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=jks,"
            + "auth-data-jks-path=/opt/keystore.jks,"
            + "auth-data-jks-password=changeit,"
            + "auth-data-jks-alias=ozone-creds");
    assertEquals("jks", config.getProviderName());
    assertEquals("/opt/keystore.jks", config.getJksPath());
    assertEquals("changeit", config.getJksPassword());
    assertEquals("ozone-creds", config.getJksAlias());
  }

  @Test
  void flagWithoutValue() {
    AgentConfig config = AgentArgsParser.parse("auth-qr");
    assertTrue(config.isQrEnabled());
  }

  @Test
  void extraWhitespace() {
    AgentConfig config = AgentArgsParser.parse(
        " auth-data-provider=env , auth-qr=true ");
    assertEquals("env", config.getProviderName());
    assertTrue(config.isQrEnabled());
  }

  @Test
  void unknownArgsIgnored() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env,unknown-key=value");
    assertEquals("env", config.getProviderName());
  }

  @Test
  void bundleCredsDefaultFalse() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env");
    assertFalse(config.isBundleCreds());
  }

  @Test
  void bundleCredsExplicitTrue() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env,auth-bundle-creds=true");
    assertTrue(config.isBundleCreds());
  }

  @Test
  void offlineAccessDefaultFalse() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env");
    assertFalse(config.isOfflineAccess());
  }

  @Test
  void offlineAccessExplicitTrue() {
    AgentConfig config = AgentArgsParser.parse(
        "auth-data-provider=env,auth-offline-access=true");
    assertTrue(config.isOfflineAccess());
  }

  @Test
  void offlineAccessFlagOnly() {
    AgentConfig config = AgentArgsParser.parse("auth-offline-access");
    assertTrue(config.isOfflineAccess());
  }
}
