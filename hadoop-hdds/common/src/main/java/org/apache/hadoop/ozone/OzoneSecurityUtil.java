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

package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.apache.commons.validator.routines.InetAddressValidator;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY;

import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone security Util class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class OzoneSecurityUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneSecurityUtil.class);
  // List of ip's not recommended to be added to CSR.
  private static final Set<String> INVALID_IPS = new HashSet<>(Arrays.asList(
      "0.0.0.0", "127.0.0.1"));

  private OzoneSecurityUtil() {
  }

  public static boolean isSecurityEnabled(ConfigurationSource conf) {
    return conf.getBoolean(OZONE_SECURITY_ENABLED_KEY,
        OZONE_SECURITY_ENABLED_DEFAULT);
  }

  /**
   * Whether Kerberos is required on the external surfaces: OM client RPC
   * (ofs/o3fs and {@code ozone s3 getsecret}) and the S3 Gateway HTTP listener
   * (SPNEGO and AWS Sig V4). Falls back to {@link #isSecurityEnabled} when the
   * dedicated key is not set, so existing single-flag deployments behave
   * unchanged.
   */
  public static boolean isExternalKerberosEnabled(ConfigurationSource conf) {
    return conf.getBoolean(OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY,
        isSecurityEnabled(conf));
  }

  /**
   * Whether Kerberos is required on inter-service RPCs and the daemon keytab
   * login of OM/SCM/DN/Recon. Falls back to {@link #isSecurityEnabled} when
   * the dedicated key is not set.
   */
  public static boolean isInterServiceKerberosEnabled(
      ConfigurationSource conf) {
    return conf.getBoolean(OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY,
        isSecurityEnabled(conf));
  }

  /**
   * True when the daemon must perform a keytab Kerberos login at startup —
   * i.e. when at least one of its RPC surfaces (external client or
   * inter-service) is configured to use Kerberos. The single keytab loaded
   * here covers whichever surface(s) are Kerberos-enabled; for example, in
   * the {@code external=true, interservice=false} split-Kerberos deployment
   * the daemon still needs its external-facing SPN to accept Sig V4/SPNEGO
   * and ofs Kerberos handshakes, but does not require an inter-service SPN.
   */
  public static boolean requiresDaemonKerberosLogin(
      ConfigurationSource conf) {
    return isExternalKerberosEnabled(conf)
        || isInterServiceKerberosEnabled(conf);
  }

  /**
   * True when a daemon that has *no external Kerberos surface* of its own
   * (SCM, DN, Recon, and S3G when HTTP SPNEGO is off) needs to perform the
   * keytab login at startup. In split-Kerberos mode these daemons only speak
   * SIMPLE inter-service traffic, so the only reason for them to load a
   * keytab is when inter-service Kerberos itself is configured on.
   *
   * <p>Use this instead of {@link #requiresDaemonKerberosLogin} on daemons
   * whose Kerberos identity is never advertised to external clients. This is
   * the gate that lets operators deploy split-Kerberos without distributing
   * keytabs to every Ozone pod — only OM (and S3G if SPNEGO is on) need one.
   */
  public static boolean requiresInterServiceKerberosLogin(
      ConfigurationSource conf) {
    return isInterServiceKerberosEnabled(conf);
  }

  /**
   * Switches Hadoop's keytab login into "acceptor-only" mode so the daemon
   * reads its long-term key from the keytab WITHOUT contacting the KDC at
   * startup. Safe only when the daemon will not initiate outbound Kerberos
   * calls — i.e. in the split-Kerberos mode where inter-service traffic is
   * SIMPLE end-to-end (Step C-2, Step D). In that mode the daemon is a pure
   * Kerberos acceptor on its external port: it decrypts the client's service
   * ticket using its own keytab key (no KDC), and never needs a TGT to talk
   * to anything else.
   *
   * <p>Mechanism: Hadoop's {@code UserGroupInformation} builds its
   * Krb5LoginModule options from a private static map with
   * {@code isInitiator=true} hardcoded. We reflect into that map and flip
   * {@code isInitiator=false} before the daemon's first call to
   * {@code loginUserFromKeytab}. With {@code isInitiator=false}, the JAAS
   * login still reads the keytab into the Subject but does not perform an
   * AS-REQ to acquire a TGT — and without a TGT the UGI auto-renewer thread
   * never spawns, so the daemon's KDC traffic over its entire lifetime is
   * exactly zero packets.
   *
   * <p>No-op if any of:
   * <ul>
   *   <li>External Kerberos is off — the daemon won't run a Kerberos login
   *       at all, the optimisation is moot.</li>
   *   <li>Inter-service Kerberos is on — the daemon does need outbound
   *       Kerberos calls and therefore a TGT; leave the default initiator
   *       login alone.</li>
   *   <li>{@link OzoneConfigKeys#OZONE_SECURITY_KERBEROS_ACCEPTOR_ONLY_ENABLED_KEY}
   *       is explicitly {@code false} — the JVM has an in-process outbound
   *       Kerberos consumer (e.g. the Ranger plugin's SPNEGO REST poll when
   *       {@code ranger.plugin.<service>.forceNonKerberos=false}) that needs
   *       a TGT to sign its outbound calls. Without that TGT the consumer
   *       silently breaks — typically the Ranger plugin keeps 401-ing
   *       ranger-admin and ACL policies freeze at the boot snapshot.</li>
   * </ul>
   *
   * <p>Reflection failure (Hadoop UGI internals change) downgrades to a
   * WARN — the daemon falls back to the default (one AS-REQ at startup).
   */
  public static void useKerberosAcceptorOnlyMode(
      ConfigurationSource conf, Logger daemonLog) {
    if (!isExternalKerberosEnabled(conf)) {
      return;
    }
    if (isInterServiceKerberosEnabled(conf)) {
      return;
    }
    if (!conf.getBoolean(
        OzoneConfigKeys.OZONE_SECURITY_KERBEROS_ACCEPTOR_ONLY_ENABLED_KEY,
        OzoneConfigKeys.OZONE_SECURITY_KERBEROS_ACCEPTOR_ONLY_ENABLED_DEFAULT)) {
      daemonLog.info("Kerberos acceptor-only mode disabled by config "
              + "({}=false); daemon will perform a normal AS-REQ at startup "
              + "+ TGT renewer. Set this when a kerberized in-JVM consumer "
              + "(e.g. Ranger plugin SPNEGO) needs an outbound TGT.",
          OzoneConfigKeys.OZONE_SECURITY_KERBEROS_ACCEPTOR_ONLY_ENABLED_KEY);
      return;
    }
    try {
      // Hadoop UGI builds the Krb5 login options dynamically inside the
      // private inner class HadoopConfiguration#getKerberosEntry() by
      // copying a private-static "BASIC_JAAS_OPTIONS" HashMap and adding
      // entries on top. Krb5LoginModule's own default for isInitiator is
      // true; the dynamic builder never explicitly sets it, so anything
      // we add to BASIC_JAAS_OPTIONS propagates into every subsequent
      // login. Putting "isInitiator=false" there is the smallest possible
      // intervention that gives us a no-KDC keytab login.
      Class<?> hadoopConf = Class.forName(
          "org.apache.hadoop.security.UserGroupInformation$HadoopConfiguration");
      Field optionsField = hadoopConf.getDeclaredField("BASIC_JAAS_OPTIONS");
      optionsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, String> options =
          (Map<String, String>) optionsField.get(null);
      String previous = options.put("isInitiator", "false");
      daemonLog.info("Krb5LoginModule isInitiator switched: {} -> false. "
              + "Daemon will load its keytab without contacting the KDC; "
              + "no AS-REQ at startup, no TGT renewer.",
          previous);
    } catch (ReflectiveOperationException e) {
      daemonLog.warn("Could not switch Krb5LoginModule into acceptor-only "
              + "mode: {}. The daemon will perform a normal AS-REQ at "
              + "startup. Hadoop UGI internals may have changed.",
          e.toString());
    }
  }

  /**
   * Validates the combination of the three Kerberos flags and emits an
   * advisory WARN when the cluster is running in the split-Kerberos mode
   * (external != interservice). Rejects the {@code external=false,
   * interservice=true} combination outright — block-token and delegation-token
   * issuance to ofs/o3fs clients would silently break, since DT issuance
   * requires a Kerberos-authenticated caller on the external port.
   *
   * <p>When inter-service Kerberos is off, also auto-sets
   * {@code ipc.client.fallback-to-simple-auth-allowed=true} on {@code conf}.
   * This is the load-bearing flag that lets OM/DN/Recon's outbound RPCs to
   * SCM downgrade to SIMPLE: the JVM-global UGI stays in KERBEROS mode (it
   * has to, to power the external surface), so every outbound RPC begins
   * with a Kerberos negotiate; without this flag the IPC client refuses to
   * accept SCM's "I want SIMPLE" response and the connection dies with
   * "this client is configured to only allow secure connections". Setting it
   * here means operators don't need to remember it in their {@code core-site}.
   *
   * @throws IllegalArgumentException when the unsupported combo is set.
   */
  public static void validateKerberosFlags(ConfigurationSource conf,
      Logger daemonLog) {
    boolean external = isExternalKerberosEnabled(conf);
    boolean inter = isInterServiceKerberosEnabled(conf);
    if (!external && inter) {
      throw new IllegalArgumentException(
          OZONE_SECURITY_KERBEROS_EXTERNAL_ENABLED_KEY + "=false combined "
              + "with " + OZONE_SECURITY_KERBEROS_INTERSERVICE_ENABLED_KEY
              + "=true is not supported: ofs/o3fs clients would be unable to "
              + "bootstrap a delegation token because the external RPC port "
              + "would run without Kerberos. Either enable external Kerberos "
              + "or disable both layers.");
    }
    if (external != inter) {
      daemonLog.warn("Ozone is running in split-Kerberos mode "
          + "(external={}, interservice={}); inter-service authenticity must "
          + "be provided by the deployment (e.g. an Istio service mesh) "
          + "because the inter-service RPC port will not enforce Kerberos.",
          external, inter);
    }
    if (!inter && conf instanceof OzoneConfiguration) {
      OzoneConfiguration ozoneConf = (OzoneConfiguration) conf;
      String key = CommonConfigurationKeys
          .IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY;
      if (!ozoneConf.getBoolean(key, false)) {
        daemonLog.info("Auto-setting {}=true so this daemon's outbound RPCs "
            + "can downgrade to SIMPLE against SCM ports running in the "
            + "split-Kerberos mode.", key);
        ozoneConf.setBoolean(key, true);
      }
    }
  }

  public static boolean isHttpSecurityEnabled(ConfigurationSource conf) {
    return isSecurityEnabled(conf) &&
        conf.getBoolean(OZONE_HTTP_SECURITY_ENABLED_KEY,
        OZONE_HTTP_SECURITY_ENABLED_DEFAULT);
  }

  /**
   * Returns Keys status.
   *
   * @return True if the key files exist.
   */
  public static boolean checkIfFileExist(Path path, String fileName) {
    File dir = path.toFile();
    return dir.exists()
        && new File(dir, fileName).exists();
  }

  /**
   * Iterates through network interfaces and return all valid ip's not
   * listed in CertificateSignRequest#INVALID_IPS.
   *
   * @return List<InetAddress>
   * @throws IOException if no network interface are found or if an error
   * occurs.
   */
  public static List<InetAddress> getValidInetsForCurrentHost()
      throws IOException {
    List<InetAddress> hostIps = new ArrayList<>();
    InetAddressValidator ipValidator = InetAddressValidator.getInstance();

    Enumeration<NetworkInterface> enumNI =
        NetworkInterface.getNetworkInterfaces();
    if (enumNI == null) {
      throw new IOException("Unable to get network interfaces.");
    }

    while (enumNI.hasMoreElements()) {
      NetworkInterface ifc = enumNI.nextElement();
      if (ifc.isUp()) {
        Enumeration<InetAddress> enumAdds = ifc.getInetAddresses();
        while (enumAdds.hasMoreElements()) {
          InetAddress addr = enumAdds.nextElement();

          String hostAddress = addr.getHostAddress();
          if (!INVALID_IPS.contains(hostAddress)
              && ipValidator.isValid(hostAddress)) {
            LOG.info("Adding ip:{},host:{}", hostAddress, addr.getHostName());
            hostIps.add(addr);
          } else {
            LOG.info("ip:{} not returned.", hostAddress);
          }
        }
      }
    }

    return hostIps;
  }

  /**
   * Convert list of string encoded certificates to list of X509Certificate.
   * @param pemEncodedCerts
   * @return list of X509Certificate.
   * @throws IOException
   */
  public static List<X509Certificate> convertToX509(
      List<String> pemEncodedCerts) throws IOException {
    List<X509Certificate> x509Certificates =
        new ArrayList<>(pemEncodedCerts.size());
    for (String cert : pemEncodedCerts) {
      x509Certificates.add(CertificateCodec.getX509Certificate(
          cert, CertificateCodec::toIOException));
    }
    return x509Certificates;
  }
}
