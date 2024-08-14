package org.apache.hadoop.ozone.om.request.s3.security;

import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.s3.S3SecretEncryptionImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
