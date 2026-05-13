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
package org.apache.ozone.provider;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * CLI utility to create or update a JCEKS keystore with OAuth
 * credentials for use with {@link JksAuthDataProvider}.
 *
 * <p>Usage:
 * <pre>
 * java -cp security-auth-agent.jar org.apache.ozone.provider.JksUtil \
 *   &lt;keystore-path&gt; &lt;store-password&gt; &lt;alias&gt; \
 *   &lt;server_url&gt; &lt;login&gt; &lt;password&gt;
 * </pre>
 *
 * <p>This creates (or updates) a JCEKS keystore at the given path,
 * storing the credentials as a secret key entry that the
 * {@link JksAuthDataProvider} can read.
 */
public final class JksUtil {

  private JksUtil() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 6) {
      System.err.println(
          "Usage: JksUtil <keystore-path> <store-password> <alias>"
              + " <server_url> <login> <password>");
      System.err.println();
      System.err.println("Creates or updates a JCEKS keystore with"
          + " OAuth credentials.");
      System.err.println("The agent reads it with: "
          + "auth-data-provider=jks,"
          + "auth-data-jks-path=<path>,"
          + "auth-data-jks-password=<pw>,"
          + "auth-data-jks-alias=<alias>");
      System.exit(1);
    }

    String ksPath = args[0];
    char[] storePassword = args[1].toCharArray();
    String alias = args[2];
    String serverUrl = args[3];
    String login = args[4];
    String password = args[5];

    String data = "server_url=" + serverUrl + "\n"
        + "login=" + login + "\n"
        + "password=" + password + "\n";

    KeyStore ks = KeyStore.getInstance("JCEKS");

    // Load existing keystore or create new
    try (FileInputStream fis = new FileInputStream(ksPath)) {
      ks.load(fis, storePassword);
      System.out.println("Loaded existing keystore: " + ksPath);
    } catch (IOException e) {
      ks.load(null, storePassword);
      System.out.println("Creating new keystore: " + ksPath);
    }

    SecretKey key = new SecretKeySpec(
        data.getBytes("UTF-8"), "RAW");
    ks.setEntry(alias,
        new KeyStore.SecretKeyEntry(key),
        new KeyStore.PasswordProtection(storePassword));

    try (FileOutputStream fos = new FileOutputStream(ksPath)) {
      ks.store(fos, storePassword);
    }

    System.out.println("Stored credentials under alias '" + alias
        + "' in " + ksPath);
  }
}
