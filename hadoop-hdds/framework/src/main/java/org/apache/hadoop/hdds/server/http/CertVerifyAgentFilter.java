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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.ocsp.OCSPObjectIdentifiers;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x509.AccessDescription;
import org.bouncycastle.asn1.x509.AuthorityInformationAccess;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPReq;
import org.bouncycastle.cert.ocsp.OCSPReqBuilder;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.RevokedStatus;
import org.bouncycastle.cert.ocsp.SingleResp;
import org.bouncycastle.operator.DigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TLS client cert validation filter.
 *
 * Validates the client certificate CN against an allowed list and optionally
 * checks certificate revocation status via OCSP.
 */
public class CertVerifyAgentFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(CertVerifyAgentFilter.class);

  private List<String> allowedCNs;
  private List<String> includedPaths;
  private String ocspResponderUrl;
  private int connectTimeout;
  private int readTimeout;
  private X509CertificateHolder configuredIssuerCert;
  private LoadingCache<BigInteger, CertValidationResult> validationCache;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.allowedCNs = Arrays.asList(
        filterConfig.getInitParameter("cn").split(","));

    String includedPathsParam =
        filterConfig.getInitParameter("includedPaths");
    if (includedPathsParam != null && !includedPathsParam.trim().isEmpty()) {
      this.includedPaths = Arrays.asList(
          includedPathsParam.split(","));
    } else {
      this.includedPaths = java.util.Collections.emptyList();
    }

    this.ocspResponderUrl = filterConfig.getInitParameter("validationUrl");
    this.connectTimeout = parseIntParam(filterConfig, "connectTimeout", 5000);
    this.readTimeout = parseIntParam(filterConfig, "readTimeout", 5000);
    long cacheTtl = parseLongParam(filterConfig, "cacheTtl", 300);

    String issuerCertPath =
        filterConfig.getInitParameter("issuerCertPath");
    if (issuerCertPath != null && !issuerCertPath.isEmpty()) {
      try {
        byte[] certBytes = Files.readAllBytes(Paths.get(issuerCertPath));
        X509Certificate issuerX509 = CertificateCodec.getX509Certificate(
            new String(certBytes, java.nio.charset.StandardCharsets.UTF_8));
        this.configuredIssuerCert =
            new JcaX509CertificateHolder(issuerX509);
        LOG.info("Loaded issuer certificate from {}", issuerCertPath);
      } catch (Exception e) {
        throw new ServletException(
            "Failed to load issuer certificate from " + issuerCertPath, e);
      }
    }

    this.validationCache = CacheBuilder.newBuilder()
        .expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
        .maximumSize(10000)
        .build(new CacheLoader<BigInteger, CertValidationResult>() {
          @Override
          public CertValidationResult load(BigInteger serialNumber) {
            // Placeholder — actual loading is done via get() caller
            // because we need the full cert + issuer context.
            return CertValidationResult.VALID;
          }
        });

    LOG.info("CertVerifyAgentFilter initialized: ocspUrl={}, cacheTtl={}s, "
            + "connectTimeout={}ms, readTimeout={}ms, issuerCert={}, "
            + "includedPaths={}",
        ocspResponderUrl, cacheTtl, connectTimeout, readTimeout,
        configuredIssuerCert != null ? "configured" : "from chain",
        includedPaths);
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    if (!includedPaths.isEmpty()
        && servletRequest instanceof HttpServletRequest) {
      String uri = ((HttpServletRequest) servletRequest).getRequestURI();
      boolean matched = false;
      for (String prefix : includedPaths) {
        if (uri.startsWith(prefix)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }
    }
    try {
      Object session = servletRequest.getAttribute(
          "org.eclipse.jetty.servlet.request.ssl_session");
      if (!(session instanceof SSLSession)) {
        LOG.error("Session is not an instance of SSLSession");
        ((HttpServletResponse) servletResponse)
            .sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      SSLSession sslSession = (SSLSession) session;
      Certificate[] peerCertificates = sslSession.getPeerCertificates();
      if (peerCertificates == null || peerCertificates.length == 0
          || !(peerCertificates[0] instanceof X509Certificate)) {
        LOG.error("Peer certificates are empty");
        ((HttpServletResponse) servletResponse)
            .sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      X509Certificate cert = (X509Certificate) peerCertificates[0];
      X500Name x500name;
      try {
        x500name = new JcaX509CertificateHolder(cert).getSubject();
      } catch (CertificateEncodingException e) {
        throw new RuntimeException(e);
      }
      RDN cn = x500name.getRDNs(BCStyle.CN)[0];
      if (cn == null) {
        LOG.error("CN extracted from client certificate is empty, "
            + "request is not authenticated");
        ((HttpServletResponse) servletResponse)
            .sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      String cnClient =
          IETFUtils.valueToString(cn.getFirst().getValue());
      if (!allowedCNs.contains(cnClient)) {
        LOG.error("Client with CN={} is not allowed to make request",
            cnClient);
        ((HttpServletResponse) servletResponse)
            .sendError(HttpServletResponse.SC_FORBIDDEN);
        return;
      }

      // OCSP validation
      X509CertificateHolder issuerCertHolder =
          resolveIssuerCert(peerCertificates);
      String responderUrl = resolveOcspUrl(cert);
      if (issuerCertHolder != null && responderUrl != null) {
        BigInteger serialNumber = cert.getSerialNumber();
        CertValidationResult cached = validationCache.getIfPresent(
            serialNumber);
        if (cached == null) {
          try {
            cached = checkOcsp(cert, issuerCertHolder, responderUrl);
          } catch (Exception e) {
            LOG.error("OCSP check failed for CN={}, serial={}",
                cnClient, serialNumber, e);
            ((HttpServletResponse) servletResponse).sendError(
                HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                "Certificate validation service unavailable");
            return;
          }
          validationCache.put(serialNumber, cached);
        }
        if (!cached.isValid()) {
          LOG.error("Client certificate rejected for CN={}: {}",
              cnClient, cached.getReason());
          ((HttpServletResponse) servletResponse).sendError(
              HttpServletResponse.SC_FORBIDDEN, cached.getReason());
          return;
        }
      }
    } catch (SSLPeerUnverifiedException ex) {
      ((HttpServletResponse) servletResponse)
          .sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  private X509CertificateHolder resolveIssuerCert(
      Certificate[] peerCertificates) {
    if (configuredIssuerCert != null) {
      return configuredIssuerCert;
    }
    if (peerCertificates.length > 1
        && peerCertificates[1] instanceof X509Certificate) {
      try {
        return new JcaX509CertificateHolder(
            (X509Certificate) peerCertificates[1]);
      } catch (CertificateEncodingException e) {
        LOG.warn("Failed to parse issuer cert from chain", e);
      }
    }
    return null;
  }

  private String resolveOcspUrl(X509Certificate cert) {
    if (ocspResponderUrl != null && !ocspResponderUrl.isEmpty()) {
      return ocspResponderUrl;
    }
    return extractOcspUrlFromAia(cert);
  }

  private String extractOcspUrlFromAia(X509Certificate cert) {
    byte[] aiaBytes = cert.getExtensionValue(
        Extension.authorityInfoAccess.getId());
    if (aiaBytes == null) {
      return null;
    }
    try {
      DEROctetString octetString = (DEROctetString)
          DEROctetString.fromByteArray(aiaBytes);
      AuthorityInformationAccess aia =
          AuthorityInformationAccess.getInstance(
              octetString.getOctets());
      for (AccessDescription ad : aia.getAccessDescriptions()) {
        if (ad.getAccessMethod().equals(
            AccessDescription.id_ad_ocsp)) {
          GeneralName location = ad.getAccessLocation();
          if (location.getTagNo()
              == GeneralName.uniformResourceIdentifier) {
            return location.getName().toString();
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract OCSP URL from AIA extension", e);
    }
    return null;
  }

  private CertValidationResult checkOcsp(
      X509Certificate cert,
      X509CertificateHolder issuerCertHolder,
      String responderUrl) throws Exception {
    DigestCalculatorProvider digCalcProv =
        new JcaDigestCalculatorProviderBuilder().build();
    CertificateID certId = new CertificateID(
        digCalcProv.get(CertificateID.HASH_SHA1),
        issuerCertHolder,
        cert.getSerialNumber());

    OCSPReqBuilder reqBuilder = new OCSPReqBuilder();
    reqBuilder.addRequest(certId);

    BigInteger nonce = BigInteger.valueOf(System.currentTimeMillis());
    org.bouncycastle.asn1.x509.Extensions reqExtensions =
        new org.bouncycastle.asn1.x509.Extensions(
            new Extension(
                OCSPObjectIdentifiers.id_pkix_ocsp_nonce,
                false,
                new DEROctetString(nonce.toByteArray())));
    reqBuilder.setRequestExtensions(reqExtensions);

    OCSPReq ocspReq = reqBuilder.build();
    byte[] ocspReqBytes = ocspReq.getEncoded();

    byte[] ocspRespBytes = sendOcspRequest(responderUrl, ocspReqBytes);
    OCSPResp ocspResp = new OCSPResp(ocspRespBytes);

    if (ocspResp.getStatus() != OCSPResp.SUCCESSFUL) {
      throw new IOException("OCSP responder returned status: "
          + ocspResp.getStatus());
    }

    BasicOCSPResp basicResp =
        (BasicOCSPResp) ocspResp.getResponseObject();
    SingleResp[] responses = basicResp.getResponses();

    for (SingleResp singleResp : responses) {
      if (singleResp.getCertID().getSerialNumber()
          .equals(cert.getSerialNumber())) {
        CertificateStatus status = singleResp.getCertStatus();
        if (status == CertificateStatus.GOOD) {
          return CertValidationResult.VALID;
        } else if (status instanceof RevokedStatus) {
          RevokedStatus revoked = (RevokedStatus) status;
          return new CertValidationResult(false,
              "Certificate revoked at "
                  + revoked.getRevocationTime());
        } else {
          return new CertValidationResult(false,
              "Certificate status unknown");
        }
      }
    }
    return new CertValidationResult(false,
        "No OCSP response for certificate serial "
            + cert.getSerialNumber());
  }

  private byte[] sendOcspRequest(String responderUrl,
      byte[] ocspReqBytes) throws IOException {
    HttpURLConnection conn = null;
    try {
      URL url = new URL(responderUrl);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setConnectTimeout(connectTimeout);
      conn.setReadTimeout(readTimeout);
      conn.setRequestProperty("Content-Type",
          "application/ocsp-request");
      conn.setRequestProperty("Accept",
          "application/ocsp-response");
      conn.setFixedLengthStreamingMode(ocspReqBytes.length);

      try (OutputStream os = conn.getOutputStream()) {
        os.write(ocspReqBytes);
      }

      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException(
            "OCSP responder returned HTTP " + responseCode);
      }

      try (InputStream is = conn.getInputStream()) {
        return readAllBytes(is);
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  private static byte[] readAllBytes(InputStream is) throws IOException {
    java.io.ByteArrayOutputStream baos =
        new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = is.read(buf)) != -1) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  private static int parseIntParam(
      FilterConfig config, String name, int defaultValue) {
    String value = config.getInitParameter(name);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  private static long parseLongParam(
      FilterConfig config, String name, long defaultValue) {
    String value = config.getInitParameter(name);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  @Override
  public void destroy() {
    if (validationCache != null) {
      validationCache.invalidateAll();
    }
  }

  static class CertValidationResult {
    static final CertValidationResult VALID =
        new CertValidationResult(true, null);

    private final boolean valid;
    private final String reason;

    CertValidationResult(boolean valid, String reason) {
      this.valid = valid;
      this.reason = reason;
    }

    boolean isValid() {
      return valid;
    }

    String getReason() {
      return reason;
    }
  }
}
