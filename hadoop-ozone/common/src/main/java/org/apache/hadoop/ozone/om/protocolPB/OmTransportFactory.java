/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;

/**
 * Factory pattern to create object for RPC communication with OM.
 */
public interface OmTransportFactory {

  OmTransport createOmTransport(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId) throws IOException;

  /**
   * Variant for inter-service callers (S3G, Recon) that should route to OM's
   * dedicated service-RPC port — see
   * {@link OMConfigKeys#OZONE_OM_SERVICE_RPC_ADDRESS_KEY}. Existing factories
   * inherit the default implementation, which ignores {@code internalCaller}
   * and falls back to the legacy single-port path; address routing for the
   * service port is applied centrally by
   * {@link #create(ConfigurationSource, UserGroupInformation, String, boolean)}.
   */
  default OmTransport createOmTransport(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId, boolean internalCaller)
      throws IOException {
    return createOmTransport(source, ugi, omServiceId);
  }

  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    OmTransportFactory factory = createFactory(conf);

    return factory.createOmTransport(conf, ugi, omServiceId);
  }

  /**
   * Builds an OM transport whose target address depends on the caller's
   * locality. When {@code internalCaller} is {@code true} and OM exposes a
   * dedicated service-RPC address via
   * {@link OMConfigKeys#OZONE_OM_SERVICE_RPC_ADDRESS_KEY}, the configuration
   * handed to the underlying factory is cloned with port-only rewrites
   * applied so the failover proxy provider dials the SIMPLE sibling on
   * every OM pod. This is the seam through which the split-Kerberos
   * deployment mode routes Recon traffic over the SIMPLE-auth port while
   * ofs/o3fs continues to reach the Kerberos-protected client port.
   *
   * <p>In single-OM topologies the rewrite hits
   * {@link OMConfigKeys#OZONE_OM_ADDRESS_KEY}. In HA topologies the
   * non-HA key is ignored by {@code OmUtils} / the failover proxy
   * provider, so the rewrite enumerates every
   * {@code ozone.om.address.<serviceId>.<nodeId>} and replaces the port
   * on each — preserving per-pod FQDNs and only swapping the SASL port.
   *
   * <p>S3G doesn't reach this overload: its {@code OzoneClientCache}
   * applies the same rewrite directly because it goes through
   * {@link #create(ConfigurationSource, UserGroupInformation, String)}
   * (3-arg) via {@code OzoneClient}.
   */
  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId,
      boolean internalCaller) throws IOException {
    ConfigurationSource effectiveConf = conf;
    if (internalCaller) {
      effectiveConf = withOmServicePortAddressIfConfigured(conf);
    }
    OmTransportFactory factory = createFactory(effectiveConf);
    return factory.createOmTransport(effectiveConf, ugi, omServiceId,
        internalCaller);
  }

  /**
   * If {@link OMConfigKeys#OZONE_OM_SERVICE_RPC_ADDRESS_KEY} is set, return
   * a cloned configuration with that port substituted on every reachable
   * OM address key (non-HA fallback and per-node HA entries alike).
   * Otherwise return {@code conf} unchanged so legacy single-port clusters
   * are unaffected.
   */
  static ConfigurationSource withOmServicePortAddressIfConfigured(
      ConfigurationSource conf) {
    String servicePortAddr = conf.get(
        OMConfigKeys.OZONE_OM_SERVICE_RPC_ADDRESS_KEY);
    if (servicePortAddr == null || servicePortAddr.isEmpty()) {
      return conf;
    }

    int port = parsePortOrDefault(servicePortAddr,
        OMConfigKeys.OZONE_OM_SERVICE_RPC_PORT_DEFAULT);

    OzoneConfiguration internal = new OzoneConfiguration(
        OzoneConfiguration.of(conf));

    // Non-HA fallback: rewrite the plain ozone.om.address if set.
    String nonHaAddr = conf.get(OMConfigKeys.OZONE_OM_ADDRESS_KEY);
    if (nonHaAddr != null && !nonHaAddr.isEmpty()) {
      internal.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
          replacePort(nonHaAddr, port));
    }

    // HA: every per-node ozone.om.address.<serviceId>.<nodeId> needs the
    // port flipped so the failover proxy provider builds proxies pointed
    // at the SIMPLE sibling instead of the Kerberos main port.
    Collection<String> serviceIds = conf.getTrimmedStringCollection(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);
    for (String serviceId : serviceIds) {
      if (serviceId == null || serviceId.isEmpty()) {
        continue;
      }
      String nodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, serviceId);
      Collection<String> nodeIds = conf.getTrimmedStringCollection(nodesKey);
      for (String nodeId : nodeIds) {
        String addrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, serviceId, nodeId);
        String addr = conf.get(addrKey);
        if (addr != null && !addr.isEmpty()) {
          internal.set(addrKey, replacePort(addr, port));
        }
      }
    }
    return internal;
  }

  static int parsePortOrDefault(String hostPort, int def) {
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

  static String replacePort(String addr, int newPort) {
    int closingBracket = addr.lastIndexOf(']');
    int colon = addr.lastIndexOf(':');
    String host = (colon > closingBracket && colon > 0)
        ? addr.substring(0, colon) : addr;
    return host + ":" + newPort;
  }

  static OmTransportFactory createFactory(ConfigurationSource conf)
      throws IOException {
    try {
      // if configured transport class is different than the default
      // OmTransportFactory (Hadoop3OmTransportFactory), then
      // check service loader for transport class and instantiate it
      if (conf
          .get(OZONE_OM_TRANSPORT_CLASS,
              OZONE_OM_TRANSPORT_CLASS_DEFAULT) !=
          OZONE_OM_TRANSPORT_CLASS_DEFAULT) {
        ServiceLoader<OmTransportFactory> transportFactoryServiceLoader =
            ServiceLoader.load(OmTransportFactory.class);
        Iterator<OmTransportFactory> iterator =
            transportFactoryServiceLoader.iterator();
        if (iterator.hasNext()) {
          return iterator.next();
        }
      }
      return OmTransportFactory.class.getClassLoader()
          .loadClass(OZONE_OM_TRANSPORT_CLASS_DEFAULT)
          .asSubclass(OmTransportFactory.class)
          .newInstance();
    } catch (Exception ex) {
      throw new IOException(
          "Can't create the default OmTransport implementation", ex);
    }
  }

}
