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
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
   * handed to the underlying factory has {@link OMConfigKeys#OZONE_OM_ADDRESS_KEY}
   * rewritten to that service address. This is the seam through which the
   * split-Kerberos deployment mode routes S3G/Recon traffic over the
   * SIMPLE-auth port while ofs/o3fs continues to reach the Kerberos-protected
   * client port.
   *
   * <p>Note: this overload assumes a single-OM topology. HA support requires
   * a per-node service-RPC address mapping — see HddsXxx HA proxy provider —
   * and is intentionally deferred.
   */
  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId,
      boolean internalCaller) throws IOException {
    ConfigurationSource effectiveConf = conf;
    if (internalCaller) {
      String servicePortAddr = conf.get(
          OMConfigKeys.OZONE_OM_SERVICE_RPC_ADDRESS_KEY);
      if (servicePortAddr != null && !servicePortAddr.isEmpty()) {
        OzoneConfiguration internalConf = new OzoneConfiguration(
            OzoneConfiguration.of(conf));
        internalConf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, servicePortAddr);
        effectiveConf = internalConf;
      }
    }
    OmTransportFactory factory = createFactory(effectiveConf);
    return factory.createOmTransport(effectiveConf, ugi, omServiceId,
        internalCaller);
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
