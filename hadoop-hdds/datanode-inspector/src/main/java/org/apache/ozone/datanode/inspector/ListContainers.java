package org.apache.ozone.datanode.inspector;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jcraft.jsch.JSchException;
import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerReader;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(name = "list-containers", mixinStandardHelpOptions = true)
public class ListContainers extends ScmSubcommand {

  private static final Logger LOG = LoggerFactory.getLogger(ListContainers.class);

  @CommandLine.ParentCommand
  DatanodeCommand parentCommand;

  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      description = "Reports output directory"
  )
  String outDir;

  @CommandLine.Option(
      names = {"--block-page-size", "-bps"},
      description = "Page size to fetch blocks per container",
      type = Integer.class,
      defaultValue = "2147483647"
  )
  Integer blockPageSize;

  private OzoneConfiguration conf;

  private String outputDir;

  private ScmClient scmClient;

//  private ContainerTokenGenerator containerTokenGenerator;

  @Override
  protected void execute(ScmClient client) throws IOException {
    try {
      this.scmClient = client;
      Field scmOption1 = getClass().getSuperclass().getDeclaredField("scmOption");
      scmOption1.setAccessible(true);
      Field spec1 = scmOption1.get(this).getClass().getDeclaredField("spec");
      spec1.setAccessible(true);
      GenericParentCommand parent = (GenericParentCommand)
          ((CommandLine.Model.CommandSpec)(spec1.get(scmOption1.get(this)))).root().userObject();
      conf = parent.createOzoneConfiguration();


      List<DatanodeCommand.DatanodeWithAttributes> datanodes = parentCommand.getAllNodes(client);

      outputDir = "./";
      if (outDir != null) {
        outputDir = outDir;
      }
      if (!Files.exists(Paths.get(outputDir))) {
        Files.createDirectories(Paths.get(outputDir));
      }
      if (outputDir.charAt(outputDir.length() - 1) != '/') {
        outputDir += "/";
      }

      try {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        datanodeContainerInfo(datanodes.stream().filter(dnAttributes -> dnAttributes.getDatanodeDetails()
            .getIpAddress().equals(hostAddress)).findFirst().get().getDatanodeDetails());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void datanodeContainerInfo(DatanodeDetails datanodeDetails) throws JSchException, IOException {
    ContainerSet containerSet = new ContainerSet(1000);

    ContainerMetrics metrics = ContainerMetrics.create(conf);

    String firstStorageDir = getFirstStorageDir(conf);

    String datanodeUuid = datanodeDetails.getUuidString();

    String clusterId = getClusterId(firstStorageDir);

    MutableVolumeSet volumeSet = new MutableVolumeSet(datanodeUuid, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      MutableVolumeSet dbVolumeSet =
          HddsServerUtil.getDatanodeDbDirs(conf).isEmpty() ? null :
              new MutableVolumeSet(datanodeUuid, conf, null,
                  StorageVolume.VolumeType.DB_VOLUME, null);
      // load rocksDB with readOnly mode, otherwise it will fail.
      HddsVolumeUtil.loadAllHddsVolumeDbStore(
          volumeSet, dbVolumeSet, true, LOG);
    }

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();

    for (ContainerProtos.ContainerType containerType
        : ContainerProtos.ContainerType.values()) {
      final Handler handler =
          Handler.getHandlerForContainerType(
              containerType,
              conf,
              datanodeUuid,
              containerSet,
              volumeSet,
              metrics,
              containerReplicaProto -> {
              });
      handler.setClusterID(clusterId);
      handlers.put(containerType, handler);
    }

    ContainerController controller = new ContainerController(containerSet, handlers);

    List<HddsVolume> volumes = StorageVolumeUtil.getHddsVolumesList(
        volumeSet.getVolumesList());
    Iterator<HddsVolume> volumeSetIterator = volumes.iterator();

    LOG.info("Starting the read all the container metadata");

    while (volumeSetIterator.hasNext()) {
      HddsVolume volume = volumeSetIterator.next();
      LOG.info("Loading container metadata from volume " + volume.toString());
      final ContainerReader reader =
          new ContainerReader(volumeSet, volume, containerSet, conf, false);
      reader.run();
    }

    LOG.info("All the container metadata is loaded.");

    for (Container<?> container: controller.getContainers()) {
      String containerReportDir = outputDir + "datanode_" + datanodeDetails.getIpAddress() + "/container_"
          + container.getContainerData().getContainerID();
      Path containerReportPath = Paths.get(containerReportDir);
      if (!Files.exists(containerReportPath)) {
        Files.createDirectories(containerReportPath);
      }
      Stream<Path> contanerBlockFilesStream = Files.list(Paths.get(container.getContainerData().getChunksPath()))
          .filter(filename -> !Files.isDirectory(filename))
          .filter(filename -> filename.toString().endsWith(".block"));
      try (OutputStream blocksDataOutputStream = Files.newOutputStream(Paths.get(containerReportDir + "/report.json"),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
        ObjectMapper blockDataObjectMapper = new ObjectMapper();
        blockDataObjectMapper.setVisibility(blockDataObjectMapper.getSerializationConfig().getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        JsonGenerator blocksDataJsonGenerator = blockDataObjectMapper.createGenerator(blocksDataOutputStream,
            JsonEncoding.UTF8);
        blocksDataJsonGenerator.writeStartObject();
        blocksDataJsonGenerator.writePOJOField("containerInfo", container.getContainerData());
        blocksDataJsonGenerator.writeFieldName("blocks");
        blocksDataJsonGenerator.writeStartArray();
        List<BlockData> containerBlocks = listBlocks(container, -1L, blockPageSize);
        BlockData blockData = null;
        while (true) {
          assert containerBlocks != null;
          if (containerBlocks.isEmpty()) break;
          boolean nextContainer = false;
          for (BlockData containerBlock : containerBlocks) {
            if (blockData != null && blockData.getBlockID().equals(containerBlock.getBlockID())) {
              nextContainer = true;
              break;
            }
            blockData = containerBlock;
            BlockData finalBlockData = blockData;
            contanerBlockFilesStream = contanerBlockFilesStream.filter(filename ->
                !filename.toString().endsWith(finalBlockData.getBlockID().getLocalID() + ".block"));
            blocksDataJsonGenerator.writeStartObject();
            blocksDataJsonGenerator.writeFieldName("blockId");
            blocksDataJsonGenerator.writeStartObject();
            blocksDataJsonGenerator.writeNumberField("containerId", blockData.getBlockID().getContainerID());
            blocksDataJsonGenerator.writeNumberField("localId", blockData.getBlockID().getLocalID());
            blocksDataJsonGenerator.writeNumberField("blockCommitSequenceId", blockData.getBlockID().getBlockCommitSequenceId());
            blocksDataJsonGenerator.writeEndObject();

            blocksDataJsonGenerator.writeFieldName("metadata");
            blocksDataJsonGenerator.writeStartObject();

            for (Map.Entry<String, String> blockMetadataEntry : blockData.getMetadata().entrySet()) {
              blocksDataJsonGenerator.writeStringField(blockMetadataEntry.getKey(), blockMetadataEntry.getValue());
            }
            blocksDataJsonGenerator.writeEndObject();

            blocksDataJsonGenerator.writeFieldName("chunks");
            blocksDataJsonGenerator.writeStartArray();

            for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
              blocksDataJsonGenerator.writeStartObject();

              blocksDataJsonGenerator.writeStringField("chunkName", chunkInfo.getChunkName());
              blocksDataJsonGenerator.writeNumberField("offset", chunkInfo.getOffset());
              blocksDataJsonGenerator.writeNumberField("len", chunkInfo.getLen());
              blocksDataJsonGenerator.writeFieldName("checksumData");
              blocksDataJsonGenerator.writeStartObject();
              blocksDataJsonGenerator.writeStringField("type", chunkInfo.getChecksumData().getType().name());
              blocksDataJsonGenerator.writeNumberField("bytesPerChecksum", chunkInfo.getChecksumData().getBytesPerChecksum());
              blocksDataJsonGenerator.writeEndObject();

              blocksDataJsonGenerator.writeEndObject();
            }
            blocksDataJsonGenerator.writeEndArray();
            blocksDataJsonGenerator.writeNumberField("size", blockData.getSize());
            blocksDataJsonGenerator.writeEndObject();
          }
          if (nextContainer || containerBlocks.size() < blockPageSize) {
            break;
          }
          containerBlocks = listBlocks(container, blockData.getBlockID().getLocalID(), blockPageSize);
        }
        blocksDataJsonGenerator.writeEndArray();
        blocksDataJsonGenerator.writeFieldName("orphanedBlockFiles");
        String[] orphanedBlockFiles = contanerBlockFilesStream.map(blockFilePath ->
            blockFilePath.toAbsolutePath().toString()).toArray(String[]::new);
        blocksDataJsonGenerator.writeArray(orphanedBlockFiles, 0, orphanedBlockFiles.length);
        blocksDataJsonGenerator.writeEndObject();
        blocksDataJsonGenerator.close();

      }
    }
  }

  private String getFirstStorageDir(ConfigurationSource config)
      throws IOException {
    final Collection<String> storageDirs =
        HddsServerUtil.getDatanodeStorageDirs(config);

    return
        StorageLocation.parse(storageDirs.iterator().next())
            .getUri().getPath();
  }

  private String getClusterId(String storageDir) throws IOException {
    Preconditions.checkNotNull(storageDir);
    try (Stream<Path> stream = Files.list(Paths.get(storageDir, "hdds"))) {
      final Path firstStorageDirPath = stream.filter(Files::isDirectory)
          .findFirst().get().getFileName();
      if (firstStorageDirPath == null) {
        throw new IllegalArgumentException(
            "HDDS storage dir couldn't be identified!");
      }
      return firstStorageDirPath.toString();
    }
  }

  private List<BlockData> listBlocks(Container container, Long startLocalID,
                                                     int count) throws IOException {
    List<BlockData> result = null;
    KeyValueContainerData cData =
        (KeyValueContainerData) container.getContainerData();
    try (DBHandle db = BlockUtils.getDB(cData, conf)) {
      result = new ArrayList<>();
      String startKey = (startLocalID == -1) ? cData.startKeyEmpty()
          : cData.getBlockKey(startLocalID);
      List<? extends Table.KeyValue<String, BlockData>> range =
          db.getStore().getBlockDataTable()
              .getSequentialRangeKVs(startKey, count,
                  cData.containerPrefix(), cData.getUnprefixedKeyFilter());
      for (Table.KeyValue<String, BlockData> entry : range) {
        result.add(entry.getValue());
      }
    }
    return result;
  }

}
