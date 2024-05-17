package org.apache.hadoop.hdds.scm.cli.container.report;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.debug.ChunkDetails;
import org.apache.hadoop.ozone.debug.ChunkType;
import org.apache.hadoop.ozone.debug.ContainerChunkInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

/**
 * This class is responsible for writing key report information in JSON format.
 */
public class KeyReportWriter implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KeyReportWriter.class);
  private final OzoneClient client;
  private final JsonGenerator generator;
  private final OutputStream stream;
  private final KeyReportOptions keyReportOptions;

  private final OzoneConfiguration conf;

  private final boolean isVerbose;

  public KeyReportWriter(OzoneClient client, KeyReportOptions keyReportOptions, OzoneConfiguration conf,
                         boolean isVerbose) throws IOException {
    this.client = client;
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    mapper.findAndRegisterModules();
    this.stream = Files.newOutputStream(Paths.get(keyReportOptions.getOutput()),
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    this.generator = mapper.createGenerator(stream, JsonEncoding.UTF8);
    this.keyReportOptions = keyReportOptions;
    this.conf = conf;
    this.isVerbose = isVerbose;
  }

  /**
   * Writes root information about volumes to the output stream using the provided JSON generator.
   *
   * @throws IOException          if there is an error writing to the output stream
   * @throws InterruptedException if the thread is interrupted while writing
   */
  void writeRootInfo() throws IOException, InterruptedException {
    generator.writeStartObject();

    try {
      Iterator<? extends OzoneVolume> volumes = client.getObjectStore()
          .listVolumes(keyReportOptions.getVolumePrefix(), null);
      while (volumes.hasNext()) {
        OzoneVolume volume = volumes.next();
        writeVolumeInfo(volume);
      }
    } catch (Exception e) {
      generator.writeStringField("Error", e.getMessage());
    }

    generator.writeEndObject();
  }

  /**
   * Writes volume information to the output stream using the provided JSON generator.
   *
   * @param volume the OzoneVolume object representing the volume to write the information for
   * @throws IOException          if there is an error writing to the output stream
   * @throws InterruptedException if the thread is interrupted while writing
   */
  void writeVolumeInfo(OzoneVolume volume) throws IOException, InterruptedException {
    generator.writeFieldName(volume.getName());
    generator.writeStartObject();

    try {
      Iterator<? extends OzoneBucket> buckets = volume.listBuckets(keyReportOptions.getBucketPrefix());

      while (buckets.hasNext()) {
        OzoneBucket bucket = buckets.next();
        writeBucketInfo(volume, bucket);
      }
    } catch (Exception e) {
      generator.writeStringField("Error", e.getMessage());
    }

    generator.writeEndObject();
  }

  /**
   * Writes bucket information to the output stream using the provided JSON generator.
   *
   * @param volume the OzoneVolume object representing the volume of the bucket
   * @param bucket the OzoneBucket object representing the bucket to write the information for
   * @throws IOException          if there is an error writing to the output stream
   * @throws InterruptedException if the thread is interrupted while writing
   */
  void writeBucketInfo(OzoneVolume volume, OzoneBucket bucket) throws IOException, InterruptedException {
    generator.writeFieldName(bucket.getName());
    generator.writeStartObject();

    try {
      Iterator<? extends OzoneKey> keys = bucket.listKeys(keyReportOptions.getKeyPrefix());

      while (keys.hasNext()) {
        OzoneKey key = keys.next();
        writeKeyInfo(volume, bucket, key);
      }
    } catch (Exception e) {
      generator.writeStringField("Error", e.getMessage());
    }

    generator.writeEndObject();
  }

  /**
   * Writes key information to the output stream using the provided JSON generator.
   *
   * @param volume the OzoneVolume object representing the volume of the key
   * @param bucket the OzoneBucket object representing the bucket of the key
   * @param key    the OzoneKey object representing the key to write the information for
   * @throws IOException          if there is an error writing to the output stream
   * @throws InterruptedException if the thread is interrupted while writing
   */
  void writeKeyInfo(OzoneVolume volume, OzoneBucket bucket, OzoneKey key) throws IOException, InterruptedException {
    generator.writeFieldName(key.getName());
    generator.writeStartArray();
    writeKeyLocations(volume.getName(), bucket.getName(), key.getName());
    generator.writeEndArray();
  }


  /**
   * Writes the key locations to the output stream using the provided volume name, bucket name, and key name.
   *
   * @param volumeName the name of the volume
   * @param bucketName the name of the bucket
   * @param keyName    the name of the key
   * @throws IOException if there is an error writing to the output stream
   */
  private void writeKeyLocations(String volumeName, String bucketName, String keyName) throws IOException {
    try {
      List<OmKeyLocationInfo> locationInfos = getLocationInfos(volumeName, bucketName, keyName);
      for (OmKeyLocationInfo keyLocation : locationInfos) {
        generator.writeStartObject();
        generator.writeFieldName(String.valueOf(keyLocation.getLocalID()));
        generator.writeStartArray();
        writeKeyLocationInfo(keyLocation);
        generator.writeEndArray();
        generator.writeEndObject();
      }
    } catch (Exception e) {
      generator.writeStartObject();
      generator.writeStringField("Error", e.getMessage());
      generator.writeEndObject();
    }
  }

  /**
   * Writes the key location information to the output stream using the provided OmKeyLocationInfo object.
   *
   * @param keyLocation the OmKeyLocationInfo object containing the key location information
   * @throws IOException if there is an error writing to the output stream
   */
  private void writeKeyLocationInfo(OmKeyLocationInfo keyLocation) throws IOException {

    try {
      ContainerLayoutVersion containerLayoutVersion = ContainerLayoutVersion.getConfiguredVersion(conf);
      Pipeline keyPipeline = getKeyPipeline(keyLocation.getPipeline());
      Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto> locationBlocks = getLocationBlocks(keyLocation);
      Map<DatanodeDetails, ContainerProtos.ReadContainerResponseProto> readContainerResponses =
          getLocationContainerInfo(keyLocation);
      for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
          entry : locationBlocks.entrySet()) {
        ContainerChunkInfo chunkInfo = getChunkInfo(entry.getValue(), readContainerResponses,
            entry.getKey(), containerLayoutVersion, keyLocation, keyPipeline);
        generator.writeStartObject();
        generator.writeStringField("Datanode-HostName", entry.getKey()
            .getHostName());
        generator.writeStringField("Datanode-IP", entry.getKey()
            .getIpAddress());
        generator.writeNumberField("Container-ID", keyLocation.getContainerID());
        generator.writeNumberField("Block-ID", keyLocation.getLocalID());
        generator.writeObjectField("Locations", chunkInfo);
        generator.writeEndObject();
      }
    } catch (Exception e) {
      generator.writeStartObject();
      generator.writeStringField("Error", e.getMessage());
      generator.writeEndObject();
    }
  }

  /**
   * Retrieves the location information for the given key in the specified volume and bucket.
   *
   * @param volumeName the name of the volume
   * @param bucketName the name of the bucket
   * @param keyName    the name of the key
   * @return a list of OmKeyLocationInfo objects representing the location information of the key
   */
  private List<OmKeyLocationInfo> getLocationInfos(String volumeName, String bucketName, String keyName) {
    try {
      OzoneManagerProtocol ozoneManagerClient =
          client.getObjectStore().getClientProxy().getOzoneManagerClient();

      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
          .setBucketName(bucketName).setKeyName(keyName).build();
      OmKeyInfo keyInfo = ozoneManagerClient.getKeyInfo(keyArgs, false).getKeyInfo();
      OmKeyLocationInfoGroup locationInfos = keyInfo.getLatestVersionLocations();
      if (locationInfos == null) {
        return Collections.emptyList();
      } else {
        return locationInfos.getBlocksLatestVersionOnly();
      }
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  /**
   * Retrieves the location blocks for a given key location.
   *
   * @param keyLocation the OmKeyLocationInfo object containing the key location information
   * @return a map of DatanodeDetails to ContainerProtos.GetBlockResponseProto representing the location blocks
   * @throws IOException          if there is an error retrieving the location blocks
   * @throws InterruptedException if the thread is interrupted while retrieving the location blocks
   */
  private Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto> getLocationBlocks(OmKeyLocationInfo keyLocation)
      throws IOException, InterruptedException {
    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(conf)) {
      Pipeline keyPipeline = keyLocation.getPipeline();
      Pipeline pipeline;
      if (keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
        pipeline = Pipeline.newBuilder(keyPipeline)
            .setReplicationConfig(StandaloneReplicationConfig
                .getInstance(ONE)).build();
      } else {
        pipeline = keyPipeline;
      }

      XceiverClientManager xceiverClientManager = containerOperationClient.getXceiverClientManager();
      XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);
      ContainerProtos.DatanodeBlockID datanodeBlockID = keyLocation.getBlockID().getDatanodeBlockIDProtobuf();

      return ContainerProtocolCalls.getBlockFromAllNodes(xceiverClient, datanodeBlockID, keyLocation.getToken());
    } catch (InterruptedException e) {
      LOG.error("Execution interrupted due to {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new InterruptedException(e.getMessage());
    }
  }

  /**
   * Retrieves the location container information for the given key location.
   *
   * @param keyLocation the OmKeyLocationInfo object containing the key location information
   * @return a map of DatanodeDetails to ReadContainerResponseProto representing the location container information
   * @throws IOException          if there is an error retrieving the location container information
   * @throws InterruptedException if the thread is interrupted while retrieving the location container information
   */
  private Map<DatanodeDetails, ReadContainerResponseProto> getLocationContainerInfo(OmKeyLocationInfo keyLocation)
      throws IOException, InterruptedException {
    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(conf)) {
      Pipeline keyPipeline = keyLocation.getPipeline();
      Pipeline pipeline;
      if (keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
        pipeline = Pipeline.newBuilder(keyPipeline)
            .setReplicationConfig(StandaloneReplicationConfig
                .getInstance(ONE)).build();
      } else {
        pipeline = keyPipeline;
      }

      return containerOperationClient.readContainerFromAllNodes(keyLocation.getContainerID(), pipeline);
    } catch (InterruptedException e) {
      LOG.error("Execution interrupted due to {}", e.getMessage(), e);
      Thread.currentThread().interrupt();
      throw new InterruptedException(e.getMessage());
    }
  }

  /**
   * Retrieves the chunk information for a given block response.
   *
   * @param blockResponse          The GetBlockResponseProto object containing the block data.
   * @param readContainerResponses The map of DatanodeDetails to ReadContainerResponseProto objects,
   *                               containing the container data.
   * @param datanodeDetails        The DatanodeDetails object representing the datanode.
   * @param containerLayoutVersion The ContainerLayoutVersion object representing the container layout version.
   * @param keyLocation            The OmKeyLocationInfo object representing the key location.
   * @param keyPipeline            The Pipeline object representing the key pipeline.
   * @return The ContainerChunkInfo object containing the chunk information.
   * @throws IOException If an I/O error occurs.
   */
  private ContainerChunkInfo getChunkInfo(@Nullable ContainerProtos.GetBlockResponseProto blockResponse,
                                          Map<DatanodeDetails, ReadContainerResponseProto>
                                              readContainerResponses, DatanodeDetails datanodeDetails,
                                          ContainerLayoutVersion containerLayoutVersion, OmKeyLocationInfo keyLocation,
                                          Pipeline keyPipeline) throws IOException {
    HashSet<String> chunkPaths = new HashSet<>();
    List<ChunkDetails> chunkDetailsList = new ArrayList<>();

    if (blockResponse == null) {
      LOG.error("Cant execute getBlock on this node");
      return new ContainerChunkInfo();
    }
    List<ContainerProtos.ChunkInfo> tempChunks = blockResponse.getBlockData().getChunksList();
    ContainerProtos.ContainerDataProto containerData =
        readContainerResponses.get(datanodeDetails).getContainerData();
    for (ContainerProtos.ChunkInfo chunkInfo : tempChunks) {
      String fileName = containerLayoutVersion.getChunkFile(new File(
              getChunkLocationPath(containerData.getContainerPath())),
          keyLocation.getBlockID(),
          ChunkInfo.getFromProtoBuf(chunkInfo).toString()).toString();
      chunkPaths.add(fileName);
      ChunkDetails chunkDetails = new ChunkDetails();
      chunkDetails.setChunkName(fileName);
      chunkDetails.setChunkOffset(chunkInfo.getOffset());
      chunkDetailsList.add(chunkDetails);
    }

    boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;
    if (isVerbose) {
      return createContainerChunkInfoVerbose(containerData, chunkDetailsList, keyPipeline, isECKey, datanodeDetails);
    } else {
      return createContainerChunkInfo(chunkPaths, keyPipeline, isECKey, datanodeDetails);
    }
  }

  /**
   * Returns the key pipeline based on the provided pipeline.
   * If the provided pipeline type is not STAND_ALONE, a new pipeline object with the replication
   * configuration set to STAND_ALONE is returned. Otherwise, the original pipeline object is returned.
   *
   * @param keyPipeline The Pipeline object representing the key pipeline.
   * @return The Pipeline object representing the key pipeline.
   */
  private Pipeline getKeyPipeline(Pipeline keyPipeline) {
    if (keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
      return Pipeline.newBuilder(keyPipeline)
          .setReplicationConfig(StandaloneReplicationConfig
              .getInstance(ONE)).build();
    } else {
      return keyPipeline;
    }
  }

  /**
   * Creates a ContainerChunkInfo object with the given parameters.
   *
   * @param chunkPaths      The set of chunk paths.
   * @param keyPipeline     The pipeline of the key.
   * @param isECKey         Indicates if the key is an Erasure Coded key.
   * @param datanodeDetails The details of the datanode.
   * @return A ContainerChunkInfo object with the specified properties.
   */
  private ContainerChunkInfo createContainerChunkInfo(HashSet<String> chunkPaths, Pipeline keyPipeline, boolean isECKey,
                                                      DatanodeDetails datanodeDetails) {
    ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
    containerChunkInfo.setFiles(chunkPaths);
    containerChunkInfo.setPipelineID(keyPipeline.getId().getId());
    if (isECKey) {
      ChunkType blockChunksType = isECParityBlock(keyPipeline, datanodeDetails)
          ? ChunkType.PARITY
          : ChunkType.DATA;
      containerChunkInfo.setChunkType(blockChunksType);
    }
    return containerChunkInfo;
  }

  /**
   * Creates a ContainerChunkInfo object with verbose information.
   *
   * @param containerData    the container data
   * @param chunkDetailsList the list of chunk details
   * @param keyPipeline      the key pipeline
   * @param isECKey          flag indicating if it is an EC key
   * @param datanodeDetails  the datanode details
   * @return the ContainerChunkInfo object
   */
  private ContainerChunkInfo createContainerChunkInfoVerbose(ContainerProtos.ContainerDataProto containerData,
                                                             List<ChunkDetails> chunkDetailsList, Pipeline keyPipeline,
                                                             boolean isECKey, DatanodeDetails datanodeDetails) {
    ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();

    containerChunkInfoVerbose.setContainerPath(containerData.getContainerPath());
    containerChunkInfoVerbose.setPipeline(keyPipeline);
    containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
    if (isECKey) {
      ChunkType blockChunksType = isECParityBlock(keyPipeline, datanodeDetails)
          ? ChunkType.PARITY
          : ChunkType.DATA;
      containerChunkInfoVerbose.setChunkType(blockChunksType);
    }
    return containerChunkInfoVerbose;
  }

  /**
   * Retrieves the location path for the chunk within a container.
   *
   * @param containerLocation the location of the container
   * @return the location path of the chunk within the container
   */
  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  /**
   * Checks if the specified datanode belongs to an erasure coded parity block in the given pipeline.
   * <p>
   * The index of the datanode in the pipeline is 1-based.
   * For example, for RS-3-2, the data indexes are 1, 2, 3 and the parity indexes are 4, 5.
   *
   * @param pipeline The Pipeline object representing the pipeline.
   * @param dn       The DatanodeDetails object representing the datanode.
   * @return {@code true} if the datanode belongs to an erasure coded parity block, {@code false} otherwise.
   */
  private boolean isECParityBlock(Pipeline pipeline, DatanodeDetails dn) {
    //index is 1-based,
    //e.g. for RS-3-2 we will have data indexes 1,2,3 and parity indexes 4,5
    return pipeline.getReplicaIndex(dn) >
        ((ECReplicationConfig) pipeline.getReplicationConfig()).getData();
  }

  @Override
  public void close() throws IOException {
    generator.close();
    stream.close();
  }
}
