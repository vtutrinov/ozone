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
 * Holds parsed agent configuration from JVM -javaagent args.
 */
public class AgentConfig {

  private String providerName = "default";
  private String filePath;
  private String jksPath;
  private String jksPassword;
  private String jksAlias;
  private String tokenValidation = "none";
  private boolean qrEnabled;
  /**
   * If true, bundle the OAuth access/refresh token into the current
   * UGI credentials so YARN propagates it with the job. If false
   * (default), rely on the standard Hadoop delegation token flow:
   * the service (e.g., OM) issues a delegation token via its
   * existing secret manager, and YARN propagates that instead.
   * The user's OAuth credentials stay on the submitter machine.
   */
  private boolean bundleCreds;
  /**
   * If true, request {@code scope=offline_access} during interactive
   * login so Keycloak issues an offline refresh token (not bound to
   * the SSO session idle timeout). Useful for long-running YARN jobs
   * where the user's session may go idle while the job is running.
   */
  private boolean offlineAccess;
  /**
   * Override for the realm component when synthesizing a
   * Kerberos-shaped principal from JWT claims (Option 2).
   * {@code null} means derive from the {@code iss} claim.
   */
  private String kerberosRealm;
  /**
   * Override for the host component when synthesizing a
   * Kerberos-shaped principal. {@code null} means use the local
   * canonical hostname.
   */
  private String kerberosHost;

  public String getProviderName() {
    return providerName;
  }

  public void setProviderName(String providerName) {
    this.providerName = providerName;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getJksPath() {
    return jksPath;
  }

  public void setJksPath(String jksPath) {
    this.jksPath = jksPath;
  }

  public String getJksPassword() {
    return jksPassword;
  }

  public void setJksPassword(String jksPassword) {
    this.jksPassword = jksPassword;
  }

  public String getJksAlias() {
    return jksAlias;
  }

  public void setJksAlias(String jksAlias) {
    this.jksAlias = jksAlias;
  }

  public String getTokenValidation() {
    return tokenValidation;
  }

  public void setTokenValidation(String tokenValidation) {
    this.tokenValidation = tokenValidation;
  }

  public boolean isQrEnabled() {
    return qrEnabled;
  }

  public void setQrEnabled(boolean qrEnabled) {
    this.qrEnabled = qrEnabled;
  }

  public boolean isBundleCreds() {
    return bundleCreds;
  }

  public void setBundleCreds(boolean bundleCreds) {
    this.bundleCreds = bundleCreds;
  }

  public boolean isOfflineAccess() {
    return offlineAccess;
  }

  public void setOfflineAccess(boolean offlineAccess) {
    this.offlineAccess = offlineAccess;
  }

  public String getKerberosRealm() {
    return kerberosRealm;
  }

  public void setKerberosRealm(String kerberosRealm) {
    this.kerberosRealm = kerberosRealm;
  }

  public String getKerberosHost() {
    return kerberosHost;
  }

  public void setKerberosHost(String kerberosHost) {
    this.kerberosHost = kerberosHost;
  }

  @Override
  public String toString() {
    return "AgentConfig{"
        + "providerName='" + providerName + '\''
        + ", filePath='" + filePath + '\''
        + ", jksPath='" + jksPath + '\''
        + ", jksAlias='" + jksAlias + '\''
        + ", tokenValidation='" + tokenValidation + '\''
        + ", qrEnabled=" + qrEnabled
        + ", bundleCreds=" + bundleCreds
        + ", offlineAccess=" + offlineAccess
        + ", kerberosRealm='" + kerberosRealm + '\''
        + ", kerberosHost='" + kerberosHost + '\''
        + '}';
  }
}
