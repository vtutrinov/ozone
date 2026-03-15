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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.server.http.CertVerifyAgentFilter.CertValidationResult;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.ocsp.OCSPObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.BasicOCSPRespBuilder;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.RevokedStatus;
import org.bouncycastle.cert.ocsp.UnknownStatus;
import org.bouncycastle.cert.ocsp.jcajce.JcaBasicOCSPRespBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the OCSP response validation of {@link CertVerifyAgentFilter} (SDPOZN-2257).
 */
public class TestCertVerifyAgentFilter {

  private static final byte[] NONCE = {1, 2, 3, 4, 5, 6, 7, 8};
  private static final long HOUR = TimeUnit.HOURS.toMillis(1);

  private static KeyPair caKeys;
  private static X509Certificate caCert;
  private static X509Certificate clientCert;
  private static KeyPair responderKeys;
  private static X509CertificateHolder responderCert;
  private static X509CertificateHolder responderCertWithoutEku;
  private static KeyPair foreignKeys;
  private static CertificateID certId;

  @BeforeAll
  static void setUp() throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    caKeys = gen.generateKeyPair();
    responderKeys = gen.generateKeyPair();
    foreignKeys = gen.generateKeyPair();
    KeyPair clientKeys = gen.generateKeyPair();

    X500Name ca = new X500Name("CN=test-ca");
    caCert = new JcaX509CertificateConverter().getCertificate(
        new JcaX509v3CertificateBuilder(ca, BigInteger.ONE, new Date(now() - HOUR), new Date(now() + HOUR), ca,
            caKeys.getPublic())
            .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
            .build(signer(caKeys.getPrivate())));
    clientCert = new JcaX509CertificateConverter().getCertificate(
        new JcaX509v3CertificateBuilder(ca, BigInteger.valueOf(42), new Date(now() - HOUR), new Date(now() + HOUR),
            new X500Name("CN=client"), clientKeys.getPublic())
            .build(signer(caKeys.getPrivate())));
    responderCert = new JcaX509v3CertificateBuilder(ca, BigInteger.valueOf(7), new Date(now() - HOUR),
        new Date(now() + HOUR), new X500Name("CN=ocsp"), responderKeys.getPublic())
        .addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_OCSPSigning))
        .build(signer(caKeys.getPrivate()));
    responderCertWithoutEku = new JcaX509v3CertificateBuilder(ca, BigInteger.valueOf(8), new Date(now() - HOUR),
        new Date(now() + HOUR), new X500Name("CN=not-ocsp"), responderKeys.getPublic())
        .build(signer(caKeys.getPrivate()));
    certId = new CertificateID(new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1),
        new JcaX509CertificateHolder(caCert), clientCert.getSerialNumber());
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static org.bouncycastle.operator.ContentSigner signer(PrivateKey key) throws Exception {
    return new JcaContentSignerBuilder("SHA256withRSA").build(key);
  }

  private static BasicOCSPResp response(KeyPair signerKeys, X509CertificateHolder[] chain, CertificateID id,
      CertificateStatus status, Date thisUpdate, Date nextUpdate, byte[] nonce) throws Exception {
    BasicOCSPRespBuilder builder = new JcaBasicOCSPRespBuilder(signerKeys.getPublic(),
        new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1));
    builder.addResponse(id, status, thisUpdate, nextUpdate);
    if (nonce != null) {
      builder.setResponseExtensions(new Extensions(new Extension(OCSPObjectIdentifiers.id_pkix_ocsp_nonce, false,
          new DEROctetString(nonce))));
    }
    return builder.build(signer(signerKeys.getPrivate()), chain, new Date());
  }

  private static CertValidationResult evaluate(BasicOCSPResp resp) throws Exception {
    return CertVerifyAgentFilter.evaluateResponse(resp, certId, caCert, NONCE, new Date());
  }

  @Test
  void goodStatusSignedByIssuer() throws Exception {
    assertTrue(evaluate(response(caKeys, null, certId, CertificateStatus.GOOD, new Date(), null, NONCE)).isValid());
  }

  @Test
  void revokedAndUnknownStatusesAreRejected() throws Exception {
    assertFalse(evaluate(response(caKeys, null, certId,
        new RevokedStatus(new Date(now() - HOUR), CRLReason.keyCompromise), new Date(), null, NONCE)).isValid());
    assertFalse(evaluate(response(caKeys, null, certId, new UnknownStatus(), new Date(), null, NONCE)).isValid());
  }

  @Test
  void responseSignedByForeignKeyIsRejected() {
    assertThrows(IOException.class, () -> evaluate(
        response(foreignKeys, null, certId, CertificateStatus.GOOD, new Date(), null, NONCE)));
  }

  @Test
  void delegatedResponderNeedsOcspSigningUsage() throws Exception {
    assertTrue(evaluate(response(responderKeys, new X509CertificateHolder[] {responderCert}, certId,
        CertificateStatus.GOOD, new Date(), null, NONCE)).isValid());
    assertThrows(IOException.class, () -> evaluate(response(responderKeys,
        new X509CertificateHolder[] {responderCertWithoutEku}, certId, CertificateStatus.GOOD, new Date(), null,
        NONCE)));
  }

  @Test
  void nonceMismatchIsRejected() {
    assertThrows(IOException.class, () -> evaluate(
        response(caKeys, null, certId, CertificateStatus.GOOD, new Date(), null, new byte[] {9, 9, 9})));
  }

  @Test
  void staleResponseIsRejected() throws Exception {
    // expired nextUpdate
    assertThrows(IOException.class, () -> evaluate(response(caKeys, null, certId, CertificateStatus.GOOD,
        new Date(now() - 3 * HOUR), new Date(now() - 2 * HOUR), NONCE)));
    // old response without nextUpdate and without nonce
    assertThrows(IOException.class, () -> evaluate(response(caKeys, null, certId, CertificateStatus.GOOD,
        new Date(now() - 2 * HOUR), null, null)));
    // pre-produced response within its validity period, without nonce
    assertTrue(evaluate(response(caKeys, null, certId, CertificateStatus.GOOD, new Date(now() - 2 * HOUR),
        new Date(now() + HOUR), null)).isValid());
  }

  @Test
  void responseForAnotherCertificateIsNotAccepted() throws Exception {
    CertificateID otherId = new CertificateID(
        new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1),
        new JcaX509CertificateHolder(caCert), BigInteger.valueOf(43));
    assertFalse(evaluate(response(caKeys, null, otherId, CertificateStatus.GOOD, new Date(), null, NONCE))
        .isValid());
  }

  private static CertVerifyAgentFilter filterFor(String includedPaths) throws Exception {
    FilterConfig config = mock(FilterConfig.class);
    when(config.getInitParameter("cn")).thenReturn("client");
    when(config.getInitParameter("includedPaths")).thenReturn(includedPaths);
    CertVerifyAgentFilter filter = new CertVerifyAgentFilter();
    filter.init(config);
    return filter;
  }

  private static HttpServletRequest request(String rawUri, String servletPath) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn(rawUri);
    when(request.getContextPath()).thenReturn("");
    when(request.getServletPath()).thenReturn(servletPath);
    // no TLS session: a checked request is rejected
    when(request.getAttribute(anyString())).thenReturn(null);
    return request;
  }

  @Test
  void includedPathsAreMatchedOnTheNormalizedPath() throws Exception {
    CertVerifyAgentFilter filter = filterFor(" /jmx , /conf");

    // raw URI tricks reach the /jmx servlet: the check must still run
    for (String rawUri : new String[] {"//jmx", "/./jmx", "/%6Amx", "/jmx"}) {
      HttpServletResponse response = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);
      filter.doFilter(request(rawUri, "/jmx"), response, chain);
      verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
      verify(chain, never()).doFilter(any(), any());
    }

    // paths that are not included (e.g. the S3 API) are not checked
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    filter.doFilter(request("/bucket/key", "/bucket/key"), response, chain);
    verify(chain).doFilter(any(), any());
    verify(response, never()).sendError(anyInt());
  }
}
