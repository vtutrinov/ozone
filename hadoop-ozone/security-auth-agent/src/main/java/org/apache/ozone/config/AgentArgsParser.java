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

/**
 * Parses the comma-separated {@code key=value} arguments passed
 * to the Java agent via {@code -javaagent:path=args}.
 */
public final class AgentArgsParser {

  private AgentArgsParser() {
  }

  public static AgentConfig parse(String agentArgs) {
    AgentConfig config = new AgentConfig();
    if (agentArgs == null || agentArgs.trim().isEmpty()) {
      return config;
    }
    String[] parts = agentArgs.split(",");
    for (String part : parts) {
      String trimmed = part.trim();
      int eq = trimmed.indexOf('=');
      if (eq < 0) {
        applyFlag(config, trimmed);
        continue;
      }
      String key = trimmed.substring(0, eq).trim();
      String value = trimmed.substring(eq + 1).trim();
      applyKeyValue(config, key, value);
    }
    return config;
  }

  private static void applyFlag(AgentConfig config, String flag) {
    if ("auth-qr".equals(flag)) {
      config.setQrEnabled(true);
    } else if ("auth-offline-access".equals(flag)) {
      config.setOfflineAccess(true);
    } else if ("auth-bundle-creds".equals(flag)) {
      config.setBundleCreds(true);
    }
  }

  private static void applyKeyValue(AgentConfig config,
      String key, String value) {
    switch (key) {
    case "auth-data-provider":
      config.setProviderName(value);
      break;
    case "auth-data-file-path":
      config.setFilePath(value);
      break;
    case "auth-data-jks-path":
      config.setJksPath(value);
      break;
    case "auth-data-jks-password":
      config.setJksPassword(value);
      break;
    case "auth-data-jks-alias":
      config.setJksAlias(value);
      break;
    case "auth-token-validation":
      config.setTokenValidation(value);
      break;
    case "auth-qr":
      config.setQrEnabled(Boolean.parseBoolean(value));
      break;
    case "auth-bundle-creds":
      config.setBundleCreds(Boolean.parseBoolean(value));
      break;
    case "auth-offline-access":
      config.setOfflineAccess(Boolean.parseBoolean(value));
      break;
    case "auth-kerberos-realm":
      config.setKerberosRealm(value);
      break;
    case "auth-kerberos-host":
      config.setKerberosHost(value);
      break;
    case "auth-log-level":
      config.setLogLevel(value);
      break;
    case "auth-token-renewal":
      config.setTokenRenewalMode(value);
      break;
    default:
      org.apache.ozone.AgentLog.warn("Unknown arg: " + key);
      break;
    }
  }
}
