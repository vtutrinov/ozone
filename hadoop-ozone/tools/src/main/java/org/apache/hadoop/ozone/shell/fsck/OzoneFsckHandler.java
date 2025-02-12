/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.fsck;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApi;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApiImpl;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;
import org.apache.hadoop.security.token.Token;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;

/**
 * OzoneFsckHandler is responsible for checking the integrity of keys in an Ozone filesystem.
 * It traverses volumes, buckets, and keys to detect and optionally to delete corrupted keys.
 */
public class OzoneFsckHandler implements AutoCloseable {

  private final OzoneFsckPathPrefix pathPrefix;

  private final OzoneFsckWriter writer;

  private final OzoneFsckVerboseSettings verboseSettings;

  private final boolean deleteCorruptedKeys;

  private final Path checkpointPath;

  private final OzoneClient client;

  private final OzoneManagerProtocol omClient;

  private final ContainerOperationClient containerOperationClient;

  private final XceiverClientManager xceiverClientManager;

  /**
   * Constructs an OzoneFsckHandler that is used
   * to perform the Ozone File System Check (fscheck) operation in the Ozone filesystem.
   *
   * @throws IOException thrown in case {@link XceiverClientManager} instance can't be acquired
   * or {@link ContainerOperationClient} can't be instantiated with provided Ozone configuration.
   */
  public OzoneFsckHandler(
      OzoneFsckPathPrefix pathPrefix,
      OzoneFsckWriter writer,
      OzoneFsckVerboseSettings verboseSettings,
      boolean deleteCorruptedKeys,
      OzoneClient client,
      ContainerOperationClient containerOperationClient,
      String checkpointPath
  ) throws IOException {
    this.pathPrefix = pathPrefix;
    this.writer = writer;
    this.verboseSettings = verboseSettings;
    this.deleteCorruptedKeys = deleteCorruptedKeys;
    this.client = client;
    this.checkpointPath = Paths.get(checkpointPath);
    this.omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
    this.containerOperationClient = containerOperationClient;
    this.xceiverClientManager = containerOperationClient.getXceiverClientManager();
  }

  /**
   * Initiates a scan operation that traverses through the volumes, buckets, and keys to perform file system checks.
   *
   * @throws IOException if an I/O error occurs during the scan process.
   */
  public void scan() throws IOException {
    initializeCheckpoint();
    scanVolumes();
    deleteCheckpoint();
  }

  private void initializeCheckpoint() throws IOException {
    try {
      if (!Files.exists(checkpointPath)) {
        Files.createFile(checkpointPath);
      }
    } catch (IOException e) {
      throw new IOException("Failed to create checkpoint file: " + checkpointPath, e);
    }
  }
  private void deleteCheckpoint() throws IOException {
    try {
      Files.delete(checkpointPath);
    } catch (IOException e) {
      throw new IOException("Failed to delete checkpoint file: " + checkpointPath, e);
    }
  }
  private OzoneFsckCheckpointState loadCheckpoint() throws IOException {
    OzoneFsckCheckpointState state = new OzoneFsckCheckpointState();
    if (Files.exists(checkpointPath)) {
      try (BufferedReader reader = Files.newBufferedReader(checkpointPath)) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] parts = line.split(": ");
          switch (parts[0]) {
          case "volumeName":
            state.setLastCheckedVolume(parts[1]);
            break;
          case "bucketName":
            state.setLastCheckedBucket(parts[1]);
            break;
          case "keyName":
            state.setLastCheckedKey(parts[1]);
            break;
          default:
            break;
          }
        }
      }
    }
    return state;
  }

  private void saveCheckpoint(String volume, String bucket, String key) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(
            checkpointPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)
    ) {
      if (volume != null) {
        writer.write("volumeName: " + volume);
        writer.newLine();
      }
      if (bucket != null) {
        writer.write("bucketName: " + bucket);
        writer.newLine();
      }
      if (key != null) {
        writer.write("keyName: " + key);
        writer.newLine();
      }
    }
  }

  private void scanVolumes() throws IOException {
    OzoneFsckCheckpointState state = loadCheckpoint();
    Iterator<? extends OzoneVolume> volumes = client.getObjectStore()
            .listVolumes(pathPrefix.volume(), state.getLastCheckedVolume());

    String lastCheckedVolume;
    String lastCheckedBucket = state.getLastCheckedBucket();
    String lastCheckedKey = state.getLastCheckedKey();

    while (volumes.hasNext()) {
      OzoneVolume volume = volumes.next();
      lastCheckedVolume = volume.getName();
      scanBuckets(volume);
      saveCheckpoint(lastCheckedVolume, lastCheckedBucket, lastCheckedKey);
    }
  }

  private void scanBuckets(OzoneVolume volume) throws IOException {
    OzoneFsckCheckpointState state = loadCheckpoint();
    String prevBucket = state.getLastCheckedBucket();

    Iterator<? extends OzoneBucket> buckets = volume.listBuckets(pathPrefix.bucket(), prevBucket);

    String lastCheckedVolume = state.getLastCheckedVolume();
    String lastCheckedBucket;
    String lastCheckedKey = state.getLastCheckedKey();

    while (buckets.hasNext()) {
      OzoneBucket bucket = buckets.next();
      lastCheckedBucket = bucket.getName();
      scanKeys(bucket);
      saveCheckpoint(lastCheckedVolume, lastCheckedBucket, lastCheckedKey);
    }
  }

  private void scanKeys(OzoneBucket bucket) throws IOException {
    OzoneFsckCheckpointState state = loadCheckpoint();
    String prevKey = state.getLastCheckedKey();

    Iterator<? extends OzoneKey> keys = bucket.listKeys(pathPrefix.key(), prevKey);

    String lastCheckedVolume = state.getLastCheckedVolume();
    String lastCheckedBucket = state.getLastCheckedBucket();
    String lastCheckedKey;

    while (keys.hasNext()) {
      OzoneKey key = keys.next();
      lastCheckedKey = key.getName();
      scanKey(key);
      saveCheckpoint(lastCheckedVolume, lastCheckedBucket, lastCheckedKey);
    }
  }

  private void scanKey(OzoneKey key) throws IOException {
    OmKeyArgs keyArgs = createKeyArgs(key);

    KeyInfoWithVolumeContext keyInfoWithContext;
    try {
      keyInfoWithContext = omClient.getKeyInfo(keyArgs, false);
    } catch (OMException omException) {
      if (omException.getResult().equals(OMException.ResultCodes.PERMISSION_DENIED)) {
        return;
      } else {
        throw omException;
      }
    }

    OmKeyInfo keyInfo = keyInfoWithContext.getKeyInfo();

    OmKeyLocationInfoGroup latestKeyInfo = keyInfo.getLatestVersionLocations();

    if (latestKeyInfo == null) {
      writer.writeCorruptedKey(keyInfo);
      return;
    }

    List<OmKeyLocationInfo> locations = latestKeyInfo.getBlocksLatestVersionOnly();

    Set<BlockID> damagedBlocks = new HashSet<>();

    for (OmKeyLocationInfo location : locations) {
      Pipeline pipeline = getKeyPipeline(location.getPipeline());

      XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);

      BlockID blockID = location.getBlockID().getProtobuf();

      try (ContainerMultinodeApi containerClient = new ContainerMultinodeApiImpl(xceiverClient)) {
        Map<DatanodeDetails, VerifyBlockResponseProto> responses = containerClient.verifyBlock(
            location.getBlockID().getDatanodeBlockIDProtobuf(),
            location.getToken()
        );

        if (responses.isEmpty()) {
          damagedBlocks.add(blockID);
        } else {
          for (VerifyBlockResponseProto response : responses.values()) {
            if (response.hasValid() && !response.getValid()) {
              damagedBlocks.add(blockID);
              break;
            }
          }

          if (!damagedBlocks.contains(blockID) && verboseSettings.printHealthyKeys()) {
            printKeyInformation(keyInfo, Collections.emptySet(), xceiverClient);
          }
        }

        if (!damagedBlocks.isEmpty()) {
          printKeyInformation(keyInfo, damagedBlocks, xceiverClient);

          if (deleteCorruptedKeys) {
            omClient.deleteKey(keyArgs);
          }
        }
      } catch (Exception e) {
        throw new IOException("Can't sent request to Datanode.", e);
      } finally {
        xceiverClientManager.releaseClientForReadData(xceiverClient, false);
      }
    }
  }

  private void printKeyInformation(OmKeyInfo keyInfo, Set<BlockID> damagedBlocks, XceiverClientSpi xceiverClient)
      throws IOException {
    boolean healthyKey = damagedBlocks.isEmpty();
    if (!healthyKey || verboseSettings.printHealthyKeys()) {
      writer.writeKeyInfo(keyInfo, () -> {
        writer.writeDamagedBlocks(damagedBlocks);

        printKeyDetails(keyInfo, xceiverClient);
      });
    }
  }

  private void printKeyDetails(OmKeyInfo keyInfo, XceiverClientSpi xceiverClient) throws IOException {
    if (verboseSettings.printContainers()) {
      OmKeyLocationInfoGroup locationInfoGroup = Objects.requireNonNull(keyInfo.getLatestVersionLocations());

      for (OmKeyLocationInfo locationInfo : locationInfoGroup.getBlocksLatestVersionOnly()) {
        Pipeline pipeline = locationInfo.getPipeline();

        if (pipeline.getType() != ReplicationType.STAND_ALONE && pipeline.getType() != ReplicationType.EC) {
          pipeline = Pipeline.newBuilder(pipeline)
              .setReplicationConfig(
                  StandaloneReplicationConfig.getInstance(
                      ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig())))
              .build();
        }

        writer.writeLocationInfo(locationInfo);

        Map<DatanodeDetails, ReadContainerResponseProto> readContainerResponses =
            readContainerInfos(locationInfo, pipeline);

        for (Map.Entry<DatanodeDetails, ReadContainerResponseProto> entry : readContainerResponses.entrySet()) {
          ContainerDataProto containerInfo = entry.getValue().getContainerData();
          DatanodeDetails datanodeDetails = entry.getKey();

          writer.writeContainerInfo(
              containerInfo,
              datanodeDetails,
              () -> printContainerDetails(locationInfo, xceiverClient)
          );
        }
      }
    }
  }

  private Map<DatanodeDetails, ReadContainerResponseProto> readContainerInfos(OmKeyLocationInfo locationInfo,
      Pipeline pipeline) throws IOException {
    try {
      return containerOperationClient.readContainerFromAllNodes(locationInfo.getContainerID(), pipeline);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void printContainerDetails(OmKeyLocationInfo locationInfo, XceiverClientSpi xceiverClient)
      throws IOException {
    if (verboseSettings.printBlocks()) {
      BlockData blockInfo = getChunksForLocation(locationInfo, xceiverClient);

      writer.writeBlockInfo(blockInfo, () -> printBlockDetails(blockInfo));
    }
  }

  private void printBlockDetails(BlockData blockInfo) throws IOException {
    if (verboseSettings.printChunks()) {
      List<ChunkInfo> chunkList = blockInfo.getChunksList();

      writer.writeChunkInfo(chunkList);
    }
  }

  private BlockData getChunksForLocation(OmKeyLocationInfo keyLocationInfo, XceiverClientSpi xceiverClient)
      throws IOException {
    Token<OzoneBlockTokenIdentifier> token = keyLocationInfo.getToken();
    Pipeline pipeline = keyLocationInfo.getPipeline();

    if (pipeline.getType() != ReplicationType.STAND_ALONE && pipeline.getType() != ReplicationType.EC) {
      pipeline = Pipeline.newBuilder(pipeline)
          .setReplicationConfig(StandaloneReplicationConfig
              .getInstance(ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig())))
          .build();
    }

    GetBlockResponseProto response = ContainerProtocolCalls.getBlock(
        xceiverClient,
        keyLocationInfo.getBlockID(),
        token,
        pipeline.getReplicaIndexes()
    );

    return response.getBlockData();
  }

  private Pipeline getKeyPipeline(Pipeline keyPipeline) {
    boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == ReplicationType.EC;
    if (!isECKey && keyPipeline.getType() != STAND_ALONE) {
      return Pipeline.newBuilder(keyPipeline)
              .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
              .build();
    } else {
      return keyPipeline;
    }
  }

  private OmKeyArgs createKeyArgs(OzoneKey key) {
    return new OmKeyArgs.Builder()
        .setVolumeName(key.getVolumeName())
        .setBucketName(key.getBucketName())
        .setKeyName(key.getName())
        .build();
  }

  @Override
  public void close() throws Exception {
    this.xceiverClientManager.close();
    this.containerOperationClient.close();
  }
}
