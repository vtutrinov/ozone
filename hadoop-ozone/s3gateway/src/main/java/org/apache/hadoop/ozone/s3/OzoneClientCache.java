/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY;

/**
 * Cached ozone client for s3 requests.
 */
@ApplicationScoped
public final class OzoneClientCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientCache.class);
  // single, cached OzoneClient established on first connection
  // for s3g gRPC OmTransport, OmRequest - OmResponse channel
  private static volatile OzoneClientCache instance;
  private OzoneClient client;
  private SecurityConfig secConfig;

  private OzoneClientCache(OzoneConfiguration ozoneConfiguration)
      throws IOException {
    // Set the expected OM version if not set via config.
    ozoneConfiguration.setIfUnset(OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
        OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT);
    String omServiceID = OmUtils.getOzoneManagerServiceId(ozoneConfiguration);
    secConfig = new SecurityConfig(ozoneConfiguration);
    client = null;

    // S3G is an inter-service caller of OM. When the OM advertises a
    // dedicated service-RPC port (split-Kerberos mode), prefer that
    // address so the RPC rides the SIMPLE-auth port and forwards the
    // user's S3 sigv4 identity via the per-request S3Auth header.
    OzoneConfiguration clientConf =
        withOmServicePortAddressIfConfigured(ozoneConfiguration);

    try {
      if (secConfig.isGrpcTlsEnabled()) {
        if (clientConf
            .get(OZONE_OM_TRANSPORT_CLASS,
                OZONE_OM_TRANSPORT_CLASS_DEFAULT) !=
            OZONE_OM_TRANSPORT_CLASS_DEFAULT) {
          // Grpc transport selected
          // need to get certificate for TLS through
          // hadoop rpc first via ServiceInfo
          setCertificate(omServiceID,
              clientConf);
        }
      }
      if (omServiceID == null) {
        client = OzoneClientFactory.getRpcClient(clientConf);
      } else {
        // As in HA case, we need to pass om service ID.
        client = OzoneClientFactory.getRpcClient(omServiceID,
            clientConf);
      }
    } catch (IOException e) {
      LOG.warn("cannot create OzoneClient", e);
      throw e;
    }
    // S3 Gateway should always set the S3 Auth.
    ozoneConfiguration.setBoolean(S3Auth.S3_AUTH_CHECK, true);
  }

  /**
   * Returns a copy of {@code base} with every OM address rewritten so its
   * port matches {@code ozone.om.service.rpc-address}, leaving the original
   * conf untouched. In HA mode each per-node address
   * ({@code ozone.om.address.<service>.<nodeId>}) is rewritten so the
   * failover proxy provider dials the SIMPLE-auth sibling port on every
   * OM pod. In non-HA mode the plain {@code ozone.om.address} is rewritten.
   *
   * <p>Step N — supersedes the earlier non-HA-only rewrite. Mirrors the
   * shape of {@code HAUtils.withScmServicePortIfConfigured}: only the port
   * is taken from the configured rpc-address value, the host portion is
   * cosmetic. When the rpc-address key is unset, returns {@code base}
   * unchanged so the cluster keeps its legacy single-port behaviour.
   */
  private static OzoneConfiguration withOmServicePortAddressIfConfigured(
      OzoneConfiguration base) {
    String servicePortAddr = base.get(
        OMConfigKeys.OZONE_OM_SERVICE_RPC_ADDRESS_KEY);
    if (servicePortAddr == null || servicePortAddr.isEmpty()) {
      return base;
    }

    int port = parsePortOrDefault(servicePortAddr,
        OMConfigKeys.OZONE_OM_SERVICE_RPC_PORT_DEFAULT);

    OzoneConfiguration internal = new OzoneConfiguration(base);

    // Non-HA fallback: rewrite the plain ozone.om.address if it's set.
    String nonHaAddr = base.get(OMConfigKeys.OZONE_OM_ADDRESS_KEY);
    if (nonHaAddr != null && !nonHaAddr.isEmpty()) {
      internal.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
          replacePort(nonHaAddr, port));
    }

    // HA: enumerate every ozone.om.address.<serviceId>.<nodeId> and rewrite
    // it so the failover proxy provider hits the SIMPLE sibling on every
    // OM pod. We read the service IDs directly rather than going through
    // OmUtils.getOzoneManagerServiceId — that helper throws when more than
    // one service ID is declared without an explicit internal-id override,
    // which is the very situation S3G is built for. Iterating every
    // declared service ID is safe: each one's per-node keys are namespaced.
    Collection<String> serviceIds = base.getTrimmedStringCollection(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);
    for (String serviceId : serviceIds) {
      if (serviceId == null || serviceId.isEmpty()) {
        continue;
      }
      String nodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, serviceId);
      Collection<String> nodeIds = base.getTrimmedStringCollection(nodesKey);
      for (String nodeId : nodeIds) {
        String addrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, serviceId, nodeId);
        String addr = base.get(addrKey);
        if (addr != null && !addr.isEmpty()) {
          internal.set(addrKey, replacePort(addr, port));
        }
      }
    }
    return internal;
  }

  /**
   * Extract the port from a host:port string, falling back to {@code def}
   * when the input lacks a port suffix or it doesn't parse as an int.
   */
  private static int parsePortOrDefault(String hostPort, int def) {
    int colon = hostPort.lastIndexOf(':');
    if (colon <= 0 || colon >= hostPort.length() - 1) {
      return def;
    }
    try {
      return Integer.parseInt(hostPort.substring(colon + 1));
    } catch (NumberFormatException ignored) {
      return def;
    }
  }

  /**
   * Return {@code addr} with any port suffix replaced by {@code newPort}.
   * Handles bare hostnames (appends {@code :newPort}), host:port
   * (replaces the suffix), and IPv6 bracketed forms ({@code [::1]:9862}).
   */
  private static String replacePort(String addr, int newPort) {
    int closingBracket = addr.lastIndexOf(']');
    int colon = addr.lastIndexOf(':');
    // Treat the colon as a port separator only if it sits past any IPv6
    // bracket — otherwise it's part of the IPv6 host literal.
    String host = (colon > closingBracket && colon > 0)
        ? addr.substring(0, colon) : addr;
    return host + ":" + newPort;
  }

  public static OzoneClient getOzoneClientInstance(OzoneConfiguration ozoneConfiguration)
      throws IOException {
    if (instance == null) {
      synchronized (OzoneClientCache.class) {
        if (instance == null) {
          instance = new OzoneClientCache(ozoneConfiguration);
        }
      }
    }

    return instance.client;
  }

  public static synchronized void closeClient() throws IOException {
    if (instance != null) {
      instance.client.close();
      instance = null;
    }
  }

  private void setCertificate(String omServiceID,
                              OzoneConfiguration conf)
      throws IOException {

    // create local copy of config incase exception occurs
    // with certificate OmRequest
    OzoneConfiguration config = new OzoneConfiguration(conf);
    OzoneClient certClient;

    if (secConfig.isGrpcTlsEnabled()) {
      // set OmTransport to hadoop rpc to securely,
      // get certificates with service list request
      config.set(OZONE_OM_TRANSPORT_CLASS,
          OZONE_OM_TRANSPORT_CLASS_DEFAULT);

      if (omServiceID == null) {
        certClient = OzoneClientFactory.getRpcClient(config);
      } else {
        // As in HA case, we need to pass om service ID.
        certClient = OzoneClientFactory.getRpcClient(omServiceID,
            config);
      }
      try {
        ServiceInfoEx serviceInfoEx = certClient
            .getObjectStore()
            .getClientProxy()
            .getOzoneManagerClient()
            .getServiceInfo();

        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          String caCertPem = null;
          List<String> caCertPems = null;
          caCertPem = serviceInfoEx.getCaCertificate();
          caCertPems = serviceInfoEx.getCaCertPemList();
          if (caCertPems == null || caCertPems.isEmpty()) {
            if (caCertPem == null) {
              LOG.error("S3g received empty caCertPems from serviceInfo");
              throw new CertificateException("No caCerts found; caCertPem can" +
                  " not be null when caCertPems is empty or null");
            }
            caCertPems = Collections.singletonList(caCertPem);
          }
          GrpcOmTransport.setCaCerts(OzoneSecurityUtil
              .convertToX509(caCertPems));
        }
      } catch (CertificateException ce) {
        throw new IOException(ce);
      } catch (IOException e) {
        throw e;
      } finally {
        if (certClient != null) {
          certClient.close();
        }
      }
    }
  }


  @PreDestroy
  public void destroy() throws IOException {
    OzoneClientCache.closeClient();
  }
}
