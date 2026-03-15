/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.server.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.ocsp.OCSPObjectIdentifiers;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x509.AccessDescription;
import org.bouncycastle.asn1.x509.AuthorityInformationAccess;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
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
import org.bouncycastle.operator.ContentVerifierProvider;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TLS client cert validation filter: the client certificate CN must be allowed and, when an OCSP responder and the
 * issuer certificate are known (SDPOZN-2257), the certificate must not be revoked.
 */
public class CertVerifyAgentFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(CertVerifyAgentFilter.class);

  // allowed difference between the clocks of this server and of the OCSP responder
  private static final long MAX_CLOCK_SKEW_MS = TimeUnit.MINUTES.toMillis(5);
  // maximum age of a response without nextUpdate and without nonce
  private static final long MAX_RESPONSE_AGE_MS = TimeUnit.HOURS.toMillis(1);

  private static final SecureRandom RANDOM = new SecureRandom();

  private List<String> allowedCNs;
  private List<String> includedPaths;
  private String ocspResponderUrl;
  private int connectTimeout;
  private int readTimeout;
  private X509Certificate configuredIssuerCert;
  private Cache<CertificateID, CertValidationResult> validationCache;
  private final AtomicBoolean ocspSkipWarned = new AtomicBoolean();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.allowedCNs = Arrays.stream(filterConfig.getInitParameter("cn").split(","))
        .map(String::trim)
        .filter(cn -> !cn.isEmpty())
        .collect(Collectors.toList());

    // SDPOZN-2257: URI path prefixes the filter applies to, all paths if none (e.g. S3 API is authenticated by AWS
    // signatures and is left out)
    String includedPathsParam = filterConfig.getInitParameter("includedPaths");
    this.includedPaths = includedPathsParam == null ? Collections.emptyList()
        : Arrays.stream(includedPathsParam.split(","))
            .map(String::trim)
            .filter(p -> !p.isEmpty())
            .collect(Collectors.toList());

    this.ocspResponderUrl = filterConfig.getInitParameter("validationUrl");
    this.connectTimeout = parseIntParam(filterConfig, "connectTimeout", 5000);
    this.readTimeout = parseIntParam(filterConfig, "readTimeout", 5000);
    long cacheTtl = parseLongParam(filterConfig, "cacheTtl", 300);

    String issuerCertPath = filterConfig.getInitParameter("issuerCertPath");
    if (issuerCertPath != null && !issuerCertPath.isEmpty()) {
      try {
        byte[] certBytes = Files.readAllBytes(Paths.get(issuerCertPath));
        this.configuredIssuerCert = CertificateCodec.getX509Certificate(new String(certBytes, StandardCharsets.UTF_8));
        LOG.info("Loaded issuer certificate from {}", issuerCertPath);
      } catch (Exception e) {
        throw new ServletException("Failed to load issuer certificate from " + issuerCertPath, e);
      }
    }

    this.validationCache = CacheBuilder.newBuilder()
        .expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
        .maximumSize(10000)
        .build();

    LOG.info("CertVerifyAgentFilter initialized: ocspUrl={}, cacheTtl={}s, connectTimeout={}ms, readTimeout={}ms, "
            + "issuerCert={}, includedPaths={}", ocspResponderUrl, cacheTtl, connectTimeout, readTimeout,
        configuredIssuerCert != null ? "configured" : "from chain", includedPaths);
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    if (!includedPaths.isEmpty() && servletRequest instanceof HttpServletRequest
        && !isIncluded((HttpServletRequest) servletRequest)) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }
    final HttpServletResponse response = (HttpServletResponse) servletResponse;
    try {
      // if we check mTLS certificate from the remote peer (aka client)
      Object session = servletRequest.getAttribute("org.eclipse.jetty.servlet.request.ssl_session");
      if (!(session instanceof SSLSession)) {
        LOG.error("Session is not an instance of SSLSession");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      SSLSession sslSession = (SSLSession) session;
      Certificate[] peerCertificates = sslSession.getPeerCertificates();
      if (peerCertificates == null || peerCertificates.length == 0
          || !(peerCertificates[0] instanceof X509Certificate)) {
        LOG.error("Peer certificates are empty");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      X509Certificate cert = (X509Certificate) peerCertificates[0];
      X500Name x500name;
      try {
        x500name = new JcaX509CertificateHolder(cert).getSubject();
      } catch (CertificateEncodingException e) {
        throw new ServletException(e);
      }
      RDN[] cns = x500name.getRDNs(BCStyle.CN);
      RDN cn = cns.length == 0 ? null : cns[0];
      if (cn == null) {
        LOG.error("CN extracted from client certificate is empty, request is not authenticated");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
      String cnClient = IETFUtils.valueToString(cn.getFirst().getValue());
      if (!allowedCNs.contains(cnClient)) {
        LOG.error("Client with CN={} is not allowed to make request", cnClient);
        response.sendError(HttpServletResponse.SC_FORBIDDEN);
        return;
      }

      // SDPOZN-2257: revocation check by OCSP
      X509Certificate issuerCert = resolveIssuerCert(cert, peerCertificates);
      String responderUrl = resolveOcspUrl(cert);
      if (issuerCert == null || responderUrl == null) {
        if ((ocspResponderUrl != null && !ocspResponderUrl.isEmpty() || configuredIssuerCert != null)
            && ocspSkipWarned.compareAndSet(false, true)) {
          LOG.warn("OCSP validation is configured but skipped for CN={}: issuer certificate {}, responder URL {}",
              cnClient, issuerCert == null ? "unknown" : "known", responderUrl == null ? "unknown" : responderUrl);
        }
      } else {
        final CertificateID certId;
        try {
          certId = new CertificateID(new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1),
              new JcaX509CertificateHolder(issuerCert), cert.getSerialNumber());
        } catch (Exception e) {
          throw new ServletException("Failed to build the OCSP certificate id", e);
        }
        CertValidationResult result = validationCache.getIfPresent(certId);
        if (result == null) {
          try {
            result = checkOcsp(certId, issuerCert, responderUrl);
          } catch (Exception e) {
            LOG.error("OCSP check failed for CN={}, serial={}", cnClient, cert.getSerialNumber(), e);
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                "Certificate validation service unavailable");
            return;
          }
          validationCache.put(certId, result);
        }
        if (!result.isValid()) {
          LOG.error("Client certificate rejected for CN={}: {}", cnClient, result.getReason());
          response.sendError(HttpServletResponse.SC_FORBIDDEN, result.getReason());
          return;
        }
      }
    } catch (SSLPeerUnverifiedException ex) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  /**
   * Matches the decoded and normalized request path (as the servlet container routes it), not the raw request URI:
   * otherwise "//jmx", "/./jmx" or "/%6Amx" would reach /jmx without the check.
   */
  private boolean isIncluded(HttpServletRequest request) {
    String path = nullToEmpty(request.getContextPath()) + nullToEmpty(request.getServletPath())
        + nullToEmpty(request.getPathInfo());
    return includedPaths.stream().anyMatch(path::startsWith);
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  /**
   * The configured issuer certificate, or the next certificate of the client chain; either must have signed the
   * client certificate, so that the OCSP request and the response signature check use the real issuer key.
   */
  private X509Certificate resolveIssuerCert(X509Certificate cert, Certificate[] peerCertificates) {
    X509Certificate issuer = configuredIssuerCert;
    if (issuer == null && peerCertificates.length > 1 && peerCertificates[1] instanceof X509Certificate) {
      issuer = (X509Certificate) peerCertificates[1];
    }
    if (issuer == null) {
      return null;
    }
    try {
      cert.verify(issuer.getPublicKey());
      return issuer;
    } catch (GeneralSecurityException e) {
      LOG.warn("Certificate {} is not signed by {}", cert.getSubjectX500Principal(), issuer.getSubjectX500Principal());
      return null;
    }
  }

  private String resolveOcspUrl(X509Certificate cert) {
    if (ocspResponderUrl != null && !ocspResponderUrl.isEmpty()) {
      return ocspResponderUrl;
    }
    return extractOcspUrlFromAia(cert);
  }

  private static String extractOcspUrlFromAia(X509Certificate cert) {
    byte[] aiaBytes = cert.getExtensionValue(Extension.authorityInfoAccess.getId());
    if (aiaBytes == null) {
      return null;
    }
    try {
      DEROctetString octetString = (DEROctetString) DEROctetString.fromByteArray(aiaBytes);
      AuthorityInformationAccess aia = AuthorityInformationAccess.getInstance(octetString.getOctets());
      for (AccessDescription ad : aia.getAccessDescriptions()) {
        if (ad.getAccessMethod().equals(AccessDescription.id_ad_ocsp)) {
          GeneralName location = ad.getAccessLocation();
          if (location.getTagNo() == GeneralName.uniformResourceIdentifier) {
            return location.getName().toString();
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract OCSP URL from AIA extension", e);
    }
    return null;
  }

  private CertValidationResult checkOcsp(CertificateID certId, X509Certificate issuerCert, String responderUrl)
      throws Exception {
    byte[] nonce = new byte[16];
    RANDOM.nextBytes(nonce);
    OCSPReq ocspReq = new OCSPReqBuilder()
        .addRequest(certId)
        .setRequestExtensions(new Extensions(new Extension(OCSPObjectIdentifiers.id_pkix_ocsp_nonce, false,
            new DEROctetString(nonce))))
        .build();
    OCSPResp ocspResp = new OCSPResp(sendOcspRequest(responderUrl, ocspReq.getEncoded()));
    if (ocspResp.getStatus() != OCSPResp.SUCCESSFUL) {
      throw new IOException("OCSP responder returned status: " + ocspResp.getStatus());
    }
    return evaluateResponse((BasicOCSPResp) ocspResp.getResponseObject(), certId, issuerCert, nonce, new Date());
  }

  /**
   * Validates the OCSP response (signature, nonce, freshness) and returns the status of the certificate.
   * @throws IOException if the response cannot be trusted
   */
  @VisibleForTesting
  static CertValidationResult evaluateResponse(BasicOCSPResp basicResp, CertificateID certId,
      X509Certificate issuerCert, byte[] nonce, Date now) throws Exception {
    verifyResponseSignature(basicResp, issuerCert, now);

    // responders echo the nonce either as sent or wrapped in an OCTET STRING (RFC 8954)
    Extension nonceExt = basicResp.getExtension(OCSPObjectIdentifiers.id_pkix_ocsp_nonce);
    if (nonceExt != null) {
      byte[] value = nonceExt.getExtnValue().getOctets();
      if (!Arrays.equals(value, nonce) && !Arrays.equals(value, new DEROctetString(nonce).getEncoded())) {
        throw new IOException("OCSP response nonce does not match the request");
      }
    }

    for (SingleResp singleResp : basicResp.getResponses()) {
      if (!singleResp.getCertID().equals(certId)) {
        continue;
      }
      Date thisUpdate = singleResp.getThisUpdate();
      Date nextUpdate = singleResp.getNextUpdate();
      if (thisUpdate.getTime() > now.getTime() + MAX_CLOCK_SKEW_MS) {
        throw new IOException("OCSP response thisUpdate " + thisUpdate + " is in the future");
      }
      if (nextUpdate != null ? nextUpdate.getTime() < now.getTime() - MAX_CLOCK_SKEW_MS
          : nonceExt == null && thisUpdate.getTime() < now.getTime() - MAX_RESPONSE_AGE_MS) {
        throw new IOException("OCSP response is stale: thisUpdate=" + thisUpdate + ", nextUpdate=" + nextUpdate);
      }
      CertificateStatus status = singleResp.getCertStatus();
      if (status == CertificateStatus.GOOD) {
        return CertValidationResult.VALID;
      } else if (status instanceof RevokedStatus) {
        return new CertValidationResult(false,
            "Certificate revoked at " + ((RevokedStatus) status).getRevocationTime());
      }
      return new CertValidationResult(false, "Certificate status unknown");
    }
    return new CertValidationResult(false, "No OCSP response for certificate serial " + certId.getSerialNumber());
  }

  /**
   * The response must be signed by the issuer, or by a responder certificate issued by the issuer for OCSP signing.
   */
  private static void verifyResponseSignature(BasicOCSPResp basicResp, X509Certificate issuerCert, Date now)
      throws Exception {
    JcaContentVerifierProviderBuilder verifierBuilder = new JcaContentVerifierProviderBuilder();
    if (basicResp.isSignatureValid(verifierBuilder.build(issuerCert.getPublicKey()))) {
      return;
    }
    ContentVerifierProvider issuerVerifier = verifierBuilder.build(new JcaX509CertificateHolder(issuerCert));
    for (X509CertificateHolder responderCert : basicResp.getCerts()) {
      if (responderCert.isSignatureValid(issuerVerifier)
          && responderCert.isValidOn(now)
          && isOcspSigner(responderCert)
          && basicResp.isSignatureValid(verifierBuilder.build(responderCert))) {
        return;
      }
    }
    throw new IOException("OCSP response is not signed by the issuer or an authorized responder");
  }

  private static boolean isOcspSigner(X509CertificateHolder responderCert) {
    ExtendedKeyUsage eku = responderCert.getExtensions() == null ? null
        : ExtendedKeyUsage.fromExtensions(responderCert.getExtensions());
    return eku != null && eku.hasKeyPurposeId(KeyPurposeId.id_kp_OCSPSigning);
  }

  private byte[] sendOcspRequest(String responderUrl, byte[] ocspReqBytes) throws IOException {
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(responderUrl).openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setConnectTimeout(connectTimeout);
      conn.setReadTimeout(readTimeout);
      conn.setRequestProperty("Content-Type", "application/ocsp-request");
      conn.setRequestProperty("Accept", "application/ocsp-response");
      conn.setFixedLengthStreamingMode(ocspReqBytes.length);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(ocspReqBytes);
      }
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("OCSP responder returned HTTP " + responseCode);
      }
      try (InputStream is = conn.getInputStream()) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = is.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return out.toByteArray();
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  private static int parseIntParam(FilterConfig config, String name, int defaultValue) {
    String value = config.getInitParameter(name);
    return value == null || value.isEmpty() ? defaultValue : Integer.parseInt(value);
  }

  private static long parseLongParam(FilterConfig config, String name, long defaultValue) {
    String value = config.getInitParameter(name);
    return value == null || value.isEmpty() ? defaultValue : Long.parseLong(value);
  }

  @Override
  public void destroy() {
    if (validationCache != null) {
      validationCache.invalidateAll();
    }
  }

  static final class CertValidationResult {
    static final CertValidationResult VALID = new CertValidationResult(true, null);

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
