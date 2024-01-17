package org.apache.hadoop.hdds.scm.cli.container.report;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
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
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

/**
 * This class represents a report of the distribution of keys among containers.
 * It retrieves information about the keys dispersion among containers and writes the report in JSON format.
 */
@CommandLine.Command(name = "keysDistribution",
    description = "returns information about an keys dispersion among containers")
public class KeyReport extends Handler {
  @CommandLine.Mixin
  private KeyReportOptions keyReportOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {
    try (OutputStream stream = Files.newOutputStream(Paths.get(keyReportOptions.getOutput()),
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
          .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
          .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
          .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
          .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
      mapper.findAndRegisterModules();
      JsonGenerator generator = mapper.createGenerator(stream, JsonEncoding.UTF8);
      generator.writeStartObject();

      Iterator<? extends OzoneVolume> volumes = client.getObjectStore()
          .listVolumes(keyReportOptions.getVolumePrefix(), null);
      while (volumes.hasNext()) {
        OzoneVolume volume = volumes.next();

        generator.writeFieldName(volume.getName());
        generator.writeStartObject();

        Iterator<? extends OzoneBucket> buckets = volume.listBuckets(keyReportOptions.getBucketPrefix());

        while (buckets.hasNext()) {
          OzoneBucket bucket = buckets.next();
          Iterator<? extends OzoneKey> keys = bucket.listKeys(keyReportOptions.getKeyPrefix());

          generator.writeFieldName(bucket.getName());
          generator.writeStartObject();

          while (keys.hasNext()) {
            OzoneKey key = keys.next();

            generator.writeFieldName(key.getName());
            writeKeyLocations(client, volume.getName(), bucket.getName(), key.getName(), generator);
          }

          generator.writeEndObject();
        }

        generator.writeEndObject();
      }

      generator.writeEndObject();
      generator.close();
    }
  }

  private void writeKeyLocations(OzoneClient client, String volumeName, String bucketName, String keyName, JsonGenerator generator)
      throws IOException {
    XceiverClientManager xceiverClientManager;
    XceiverClientSpi xceiverClient;
    OzoneManagerProtocol ozoneManagerClient;
    try (ContainerOperationClient containerOperationClient = new
        ContainerOperationClient(getConf())) {
      xceiverClientManager = containerOperationClient.getXceiverClientManager();
      ozoneManagerClient =
          client.getObjectStore().getClientProxy().getOzoneManagerClient();
      List<ContainerProtos.ChunkInfo> tempchunks;
      List<ChunkDetails> chunkDetailsList = new ArrayList<>();
      HashSet<String> chunkPaths = new HashSet<>();
      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
          .setBucketName(bucketName).setKeyName(keyName).build();
      OmKeyInfo keyInfo = ozoneManagerClient.getKeyInfo(keyArgs, false).getKeyInfo();
      // querying the keyLocations.The OM is queried to get containerID and
      // localID pertaining to a given key
      List<OmKeyLocationInfo> locationInfos =
          keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
      // for zero-sized key
      if (locationInfos.isEmpty()) {
        System.out.println("No Key Locations Found");
        return;
      }
      ContainerLayoutVersion containerLayoutVersion = ContainerLayoutVersion
          .getConfiguredVersion(getConf());
      generator.writeStartArray();

      for (OmKeyLocationInfo keyLocation : locationInfos) {
        ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();
        ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
        long containerId = keyLocation.getContainerID();
        chunkPaths.clear();
        Pipeline keyPipeline = keyLocation.getPipeline();
        boolean isECKey =
            keyPipeline.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC;
        Pipeline pipeline;
        if (keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
          pipeline = Pipeline.newBuilder(keyPipeline)
              .setReplicationConfig(StandaloneReplicationConfig
                  .getInstance(ONE)).build();
        } else {
          pipeline = keyPipeline;
        }
        xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);
        // Datanode is queried to get chunk information.Thus querying the
        // OM,SCM and datanode helps us get chunk location information
        ContainerProtos.DatanodeBlockID datanodeBlockID =
            keyLocation.getBlockID().getDatanodeBlockIDProtobuf();
        // doing a getBlock on all nodes
        Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
            responses = null;
        Map<DatanodeDetails, ContainerProtos.ReadContainerResponseProto>
            readContainerResponses = null;
        try {
          responses = ContainerProtocolCalls.getBlockFromAllNodes(xceiverClient,
              datanodeBlockID, keyLocation.getToken());
          readContainerResponses =
              containerOperationClient.readContainerFromAllNodes(
                  keyLocation.getContainerID(), pipeline);
        } catch (InterruptedException e) {
          LOG.error("Execution interrupted due to " + e);
          Thread.currentThread().interrupt();
        }

        generator.writeStartObject();
        generator.writeFieldName(String.valueOf(keyLocation.getLocalID()));
        generator.writeStartArray();

        for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
            entry : responses.entrySet()) {
          chunkPaths.clear();
          if (entry.getValue() == null) {
            LOG.error("Cant execute getBlock on this node");
            continue;
          }
          tempchunks = entry.getValue().getBlockData().getChunksList();
          ContainerProtos.ContainerDataProto containerData =
              readContainerResponses.get(entry.getKey()).getContainerData();
          for (ContainerProtos.ChunkInfo chunkInfo : tempchunks) {
            String fileName = containerLayoutVersion.getChunkFile(new File(
                    getChunkLocationPath(containerData.getContainerPath())),
                keyLocation.getBlockID(),
                ChunkInfo.getFromProtoBuf(chunkInfo)).toString();
            chunkPaths.add(fileName);
            ChunkDetails chunkDetails = new ChunkDetails();
            chunkDetails.setChunkName(fileName);
            chunkDetails.setChunkOffset(chunkInfo.getOffset());
            chunkDetailsList.add(chunkDetails);
          }
          containerChunkInfoVerbose.setContainerPath(containerData
              .getContainerPath());
          containerChunkInfoVerbose.setPipeline(keyPipeline);
          containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
          containerChunkInfo.setFiles(chunkPaths);
          containerChunkInfo.setPipelineID(keyPipeline.getId().getId());
          if (isECKey) {
            ChunkType blockChunksType =
                isECParityBlock(keyPipeline, entry.getKey()) ?
                    ChunkType.PARITY : ChunkType.DATA;
            containerChunkInfoVerbose.setChunkType(blockChunksType);
            containerChunkInfo.setChunkType(blockChunksType);
          }
          generator.writeStartObject();
          generator.writeStringField("Datanode-HostName", entry.getKey()
              .getHostName());
          generator.writeStringField("Datanode-IP", entry.getKey()
              .getIpAddress());
          generator.writeNumberField("Container-ID", containerId);
          generator.writeNumberField("Block-ID", keyLocation.getLocalID());
          if (isVerbose()) {
            generator.writeObjectField("Locations", containerChunkInfoVerbose);
          } else {
            generator.writeObjectField("Locations", containerChunkInfo);
          }
          generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
        xceiverClientManager.releaseClientForReadData(xceiverClient, false);
      }
      generator.writeEndArray();
    }
  }

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  private boolean isECParityBlock(Pipeline pipeline, DatanodeDetails dn) {
    //index is 1-based,
    //e.g. for RS-3-2 we will have data indexes 1,2,3 and parity indexes 4,5
    return pipeline.getReplicaIndex(dn) >
        ((ECReplicationConfig) pipeline.getReplicationConfig()).getData();
  }
}
