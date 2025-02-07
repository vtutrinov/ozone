package org.apache.hadoop.ozone.shell.fsck;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;

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
    client = new ClientProtocolStub();
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
  void testFsckPrintHealthyKeys() throws IOException {
    Path tempCheckpoint = Files.createTempFile("checkpoint", ".txt");
    OzoneFsckVerboseSettings verboseSettings = OzoneFsckVerboseSettings.builder()
            .printHealthyKeys(true)
            .level(OzoneFsckVerbosityLevel.KEY)
            .build();

    OzoneFsckHandler handler = new OzoneFsckHandler(
            new OzoneFsckPathPrefix(null, null, null),
            writerMock,
            verboseSettings,
            false,
            clientStub,
            containerOperationStub,
            tempCheckpoint.toString()
    );
    handler.scan();

    verify(writerMock, times(1)).writeCorruptedKey(
            argThat((OmKeyInfo k) -> CORRUPTED_KEY.equals(k.getKeyName()))
    );

    verify(writerMock, times(1)).writeKeyInfo(
            argThat((OmKeyInfo k) -> HEALTHY_KEY.equals(k.getKeyName())),
            any()
    );
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
            true,
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
