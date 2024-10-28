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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.s3.S3SecretEncryptionImpl;
import org.junit.jupiter.api.Test;

/**
 * Test S3SecretEncryptionImpl.
 */
public class TestS3SecretEncryptionImpl {

  @Test
  void testSuccessCase() throws IOException {
    S3SecretEncryptionImpl encryption = new S3SecretEncryptionImpl("password");

    S3SecretValue value = S3SecretValue.of("krb/krb@krb.krb", "secret");

    S3SecretValue encrypted = encryption.encrypt(value);

    assertNotEquals(value.getAwsSecret(), encrypted.getAwsSecret());

    S3SecretValue decrypted = encryption.decrypt(encrypted);

    assertEquals(value.getAwsAccessKey(), decrypted.getAwsAccessKey());
    assertEquals(value.getAwsSecret(), decrypted.getAwsSecret());
  }

  @Test
  void testMismatchingAccessKey() throws IOException {
    S3SecretEncryptionImpl encryption = new S3SecretEncryptionImpl("password");

    S3SecretValue value1 = S3SecretValue.of("krb/krb@krb.krb", "secret");
    S3SecretValue value2 = S3SecretValue.of("user@krb.krb", "not_secret");

    S3SecretValue encrypted1 = encryption.encrypt(value1);
    S3SecretValue encrypted2 = encryption.encrypt(value2);

    assertNotEquals(encrypted1.getAwsSecret(), encrypted2.getAwsSecret());

    S3SecretValue valueMixed = S3SecretValue.of(encrypted1.getAwsAccessKey(), encrypted2.getAwsSecret());

    assertThrows(IOException.class, () -> encryption.decrypt(valueMixed));
  }

  @Test
  void testNoKey() {
    assertThrows(IllegalArgumentException.class, () -> new S3SecretEncryptionImpl(null));
    assertThrows(IllegalArgumentException.class, () -> new S3SecretEncryptionImpl(""));
  }
}
