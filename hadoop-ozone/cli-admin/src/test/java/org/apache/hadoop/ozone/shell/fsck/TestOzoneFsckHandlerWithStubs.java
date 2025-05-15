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

package org.apache.hadoop.ozone.shell.fsck;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ClientProtocolStub;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.storage.ContainerOperationClientStub;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestOzoneFsckHandlerWithStubs {

  private ContainerOperationClientStub containerOperationStub;
  private OzoneFsckWriter writerMock;
  private ClientProtocolStub client;
  private ObjectStoreStub objectStoreStub;
  private OzoneClientStub clientStub;
  private static final String VOLUME = "testVolume";
  private static final String BUCKET = "testBucket";
  private static final String HEALTHY_KEY = "healthyKey";
  private static final String CORRUPTED_KEY = "corruptedKey";

  @BeforeEach
  void setUp() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.scm.names", "localhost");
    conf.set("ozone.scm.client.address", "localhost");
    containerOperationStub = new ContainerOperationClientStub(conf);
    client = ClientProtocolStub.create();
    objectStoreStub = new ObjectStoreStub(conf, client);
    objectStoreStub.createVolume(VOLUME);
    client.createBucket(VOLUME, BUCKET);
    client.createKey(
            VOLUME,
            BUCKET,
            HEALTHY_KEY,
            10,
            ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE,
            new HashMap<>()
    );
    client.createCorruptedKey(
            VOLUME,
            BUCKET,
            CORRUPTED_KEY,
            10,
            ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE,
            new HashMap<>()
    );
    clientStub = new OzoneClientStub(objectStoreStub, client);
    writerMock = Mockito.mock(OzoneFsckWriter.class);
  }

  @Test
  void testFsckPrintCorruptedKeys() throws IOException {
    Path tempCheckpoint = Files.createTempFile("checkpoint", ".txt");
    OzoneFsckVerboseSettings verboseSettings = OzoneFsckVerboseSettings.builder()
            .printHealthyKeys(false)
            .level(OzoneFsckVerbosityLevel.KEY)
            .build();

    OzoneFsckHandler handler = new OzoneFsckHandler(
            new OzoneFsckPathPrefix(null, null, null),
            writerMock,
            verboseSettings,
            clientStub,
            containerOperationStub,
            tempCheckpoint.toString()
    );

    handler.scan();

    verify(writerMock, times(1)).writeCorruptedKey(
            argThat((OmKeyInfo k) -> CORRUPTED_KEY.equals(k.getKeyName()))
    );

    verify(writerMock, never()).writeKeyInfo(
            argThat((OmKeyInfo k) -> HEALTHY_KEY.equals(k.getKeyName())),
            any()
    );
  }
}
