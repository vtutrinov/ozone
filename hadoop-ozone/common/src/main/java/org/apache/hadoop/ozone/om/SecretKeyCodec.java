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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.Codec;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Codec implementation for {@link SecretKey} based on Java Serialization.
 */
public class SecretKeyCodec implements Codec<SecretKey> {
  @Override
  public byte[] toPersistedFormat(SecretKey secretKey) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(secretKey);
      out.flush();
      return bos.toByteArray();
    }
  }

  @Override
  public SecretKey fromPersistedFormat(byte[] rawData) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(rawData);
    try (ObjectInput in = new ObjectInputStream(bis)) {
      return (SecretKey) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public SecretKey copyObject(SecretKey object) {
    return object;
  }
}
