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
   * Returns a copy of {@code base} with {@code ozone.om.address} rewritten to
   * {@code ozone.om.service.rpc-address} when that key is set, leaving the
   * original untouched. When the service-RPC key is not configured this
   * returns {@code base} unchanged, preserving today's single-port behaviour.
   */
  private static OzoneConfiguration withOmServicePortAddressIfConfigured(
      OzoneConfiguration base) {
    String servicePortAddr = base.get(
        OMConfigKeys.OZONE_OM_SERVICE_RPC_ADDRESS_KEY);
    if (servicePortAddr == null || servicePortAddr.isEmpty()) {
      return base;
    }
    OzoneConfiguration internal = new OzoneConfiguration(base);
    internal.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, servicePortAddr);
    return internal;
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
