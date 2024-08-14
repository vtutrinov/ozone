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

package org.apache.hadoop.ozone.om.s3;

import org.apache.hadoop.ozone.om.S3SecretEncryption;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.security.EncryptorAesGcmPassword;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.IOException;
import java.security.Security;

/**
 * Implementation of {@link S3SecretEncryption} based on password encryption.
 */
public class S3SecretEncryptionImpl implements S3SecretEncryption {

  private final String masterKey;

  public S3SecretEncryptionImpl(String masterKey) {
    if (masterKey == null || masterKey.isEmpty()) {
      throw new IllegalArgumentException("Master key is not set");
    }

    this.masterKey = masterKey;
    Security.addProvider(new BouncyCastleProvider());
  }

  @Override
  public S3SecretValue encrypt(S3SecretValue s3Secret) throws IOException {
    try {
      // Add access key to the encrypted string in order to later validate that the secret has not been substituted.
      String srtToEncrypt = s3Secret.getAwsAccessKey() + "#" + s3Secret.getAwsSecret();

      String encryptedTextBase64 = EncryptorAesGcmPassword.encrypt(srtToEncrypt, masterKey);

      return S3SecretValue.of(s3Secret.getAwsAccessKey(), encryptedTextBase64);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public S3SecretValue decrypt(S3SecretValue encryptedSecret) throws IOException {
    try {
      String decryptedText = EncryptorAesGcmPassword.decrypt(encryptedSecret.getAwsSecret(), masterKey);

      String[] keyAndSecret = decryptedText.split("#");

      if (!encryptedSecret.getAwsAccessKey().equals(keyAndSecret[0])) {
        throw new IOException("Mismatching principal");
      }

      return S3SecretValue.of(encryptedSecret.getAwsAccessKey(), keyAndSecret[1]);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
