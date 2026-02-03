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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.server.http;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

/**
 * TLS client cert validation filter.
 *
 */
public class CertVerifyAgentFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(CertVerifyAgentFilter.class);
  private List<String> allowedCNs;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.allowedCNs = Arrays.asList(filterConfig.getInitParameter("cn").split(","));
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    String cnClient = "";
    try {
      // if we check mTLS certificate from the remote peer (aka client)
      Object session = servletRequest.getAttribute("org.eclipse.jetty.servlet.request.ssl_session");
      if (!(session instanceof SSLSession)) {
        LOG.error("Session is not an instance of SSLSession");
        ((HttpServletResponse)servletResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      SSLSession sslSession = (SSLSession) session;
      Certificate[] peerCertificates = sslSession.getPeerCertificates();
      if (peerCertificates == null || peerCertificates.length == 0
          || !(peerCertificates[0] instanceof X509Certificate)) {
        LOG.error("Peer certificates are empty");
        ((HttpServletResponse)servletResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      X509Certificate cert = (X509Certificate) peerCertificates[0];
      X500Name x500name = null;
      try {
        x500name = new JcaX509CertificateHolder(cert).getSubject();
      } catch (CertificateEncodingException e) {
        throw new RuntimeException(e);
      }
      RDN cn = x500name.getRDNs(BCStyle.CN)[0];
      if (cn == null) {
        LOG.error("CN extracted from client certificate is empty, request is not authenticated");
        ((HttpServletResponse)servletResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      cnClient = IETFUtils.valueToString(cn.getFirst().getValue());
      if (!allowedCNs.contains(cnClient)) {
        LOG.error("Client with CN={} is not allowed to make request", cnClient);
        ((HttpServletResponse)servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
        return;
      }
    } catch (SSLPeerUnverifiedException ex) {
      ((HttpServletResponse)servletResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  @Override
  public void destroy() {

  }

}
