/*
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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.client.storage.ContainerStorageStub;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * {@link ByteArrayOutputStream} stub aware of {@link OpenKeySession}.
 */
public class OutputStreamStub extends ByteArrayOutputStream  {
  private final OpenKeySession openKey;

  private final ContainerStorageStub containerStorageStub;

  public OutputStreamStub(OpenKeySession openKey, ContainerStorageStub containerStorageStub) {
    super();
    this.openKey = openKey;
    this.containerStorageStub = containerStorageStub;
  }

  public OmKeyInfo getKeyInfo() {
    return openKey.getKeyInfo();
  }

  @Override
  public void close() throws IOException {
    containerStorageStub.writeKey(openKey.getKeyInfo().getKeyName(), toByteArray());

    super.close();
  }
}
