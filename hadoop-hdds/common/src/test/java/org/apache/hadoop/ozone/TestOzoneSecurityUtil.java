/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the inheritance matrix for ozone.security.kerberos.{external,
 * interservice}.enabled — both fall back to ozone.security.enabled when unset,
 * and can independently override it once explicitly set. This is the contract
 * relied on by the split-Kerberos deployment mode.
 */
public class TestOzoneSecurityUtil {

  @Test
  public void defaultsAllOff() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertFalse(OzoneSecurityUtil.isSecurityEnabled(conf));
    assertFalse(OzoneSecurityUtil.isExternalKerberosEnabled(conf));
    assertFalse(OzoneSecurityUtil.isInterServiceKerberosEnabled(conf));
  }

  @Test
  public void masterFlagPropagatesToBothLayers() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    assertTrue(OzoneSecurityUtil.isSecurityEnabled(conf));
    assertTrue(OzoneSecurityUtil.isExternalKerberosEnabled(conf));
    assertTrue(OzoneSecurityUtil.isInterServiceKerberosEnabled(conf));
  }

  @Test
  public void externalCanBeEnabledIndependently() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, true);
    assertFalse(OzoneSecurityUtil.isSecurityEnabled(conf));
    assertTrue(OzoneSecurityUtil.isExternalKerberosEnabled(conf));
    assertFalse(OzoneSecurityUtil.isInterServiceKerberosEnabled(conf));
  }

  @Test
  public void interserviceCanBeDisabledWhileExternalStaysOn() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, false);
    assertTrue(OzoneSecurityUtil.isSecurityEnabled(conf));
    assertTrue(OzoneSecurityUtil.isExternalKerberosEnabled(conf));
    assertFalse(OzoneSecurityUtil.isInterServiceKerberosEnabled(conf));
  }

  @Test
  public void requiresDaemonKerberosLoginIsTrueWhenEitherLayerOn() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertFalse(OzoneSecurityUtil.requiresDaemonKerberosLogin(conf));

    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, true);
    assertTrue(OzoneSecurityUtil.requiresDaemonKerberosLogin(conf));

    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, false);
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, true);
    assertTrue(OzoneSecurityUtil.requiresDaemonKerberosLogin(conf));
  }

  @Test
  public void requiresInterServiceKerberosLoginTracksInterserviceFlag() {
    OzoneConfiguration conf = new OzoneConfiguration();
    // External-only deployment: SCM/DN/Recon/S3G need no daemon keytab.
    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, true);
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, false);
    assertFalse(OzoneSecurityUtil.requiresInterServiceKerberosLogin(conf));

    // Inter-service Kerberos on: daemons must log in.
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, true);
    assertTrue(OzoneSecurityUtil.requiresInterServiceKerberosLogin(conf));

    // Master flag only — inter-service inherits to true.
    OzoneConfiguration legacy = new OzoneConfiguration();
    legacy.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    assertTrue(OzoneSecurityUtil.requiresInterServiceKerberosLogin(legacy));

    // Defaults: everything off.
    assertFalse(OzoneSecurityUtil.requiresInterServiceKerberosLogin(
        new OzoneConfiguration()));
  }

  @Test
  public void validateRejectsExternalOffWithInterserviceOn() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, false);
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, true);
    assertThrows(IllegalArgumentException.class,
        () -> OzoneSecurityUtil.validateKerberosFlags(conf,
            LoggerFactory.getLogger(TestOzoneSecurityUtil.class)));
  }

  @Test
  public void validateAcceptsSupportedCombos() {
    org.slf4j.Logger log =
        LoggerFactory.getLogger(TestOzoneSecurityUtil.class);
    // (false, false) — fully insecure
    OzoneConfiguration off = new OzoneConfiguration();
    assertDoesNotThrow(() -> OzoneSecurityUtil.validateKerberosFlags(off, log));

    // (true, true) — legacy secure
    OzoneConfiguration both = new OzoneConfiguration();
    both.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    assertDoesNotThrow(() -> OzoneSecurityUtil.validateKerberosFlags(both,
        log));

    // (true, false) — split-Kerberos mode; WARN expected but no throw
    OzoneConfiguration split = new OzoneConfiguration();
    split.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, true);
    split.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, false);
    assertDoesNotThrow(() -> OzoneSecurityUtil.validateKerberosFlags(split,
        log));
  }

  @Test
  public void validateAutoSetsIpcFallbackWhenInterserviceOff() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY, true);
    conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY, false);
    OzoneSecurityUtil.validateKerberosFlags(conf,
        LoggerFactory.getLogger(TestOzoneSecurityUtil.class));
    // Auto-set: outbound RPCs from this daemon now downgrade to SIMPLE
    // against SCM ports running in split-Kerberos mode.
    assertTrue(conf.getBoolean(
        "ipc.client.fallback-to-simple-auth-allowed", false));
  }

  @Test
  public void validateLeavesIpcFallbackAloneWhenInterserviceOn() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    OzoneSecurityUtil.validateKerberosFlags(conf,
        LoggerFactory.getLogger(TestOzoneSecurityUtil.class));
    // No auto-set when inter-service Kerberos is on; the flag stays at its
    // default (false) so the IPC client keeps enforcing Kerberos throughout.
    assertFalse(conf.getBoolean(
        "ipc.client.fallback-to-simple-auth-allowed", false));
  }

  @Test
  public void splitKerberosMatrix() {
    boolean[] truth = {false, true};
    for (boolean master : truth) {
      for (boolean external : truth) {
        for (boolean inter : truth) {
          OzoneConfiguration conf = new OzoneConfiguration();
          conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, master);
          conf.setBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY,
              external);
          conf.setBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY,
              inter);
          assertEquals(master, OzoneSecurityUtil.isSecurityEnabled(conf));
          assertEquals(external,
              OzoneSecurityUtil.isExternalKerberosEnabled(conf));
          assertEquals(inter,
              OzoneSecurityUtil.isInterServiceKerberosEnabled(conf));
        }
      }
    }
  }
}
