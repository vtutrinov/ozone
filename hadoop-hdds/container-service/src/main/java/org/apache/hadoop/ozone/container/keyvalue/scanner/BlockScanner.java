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

package org.apache.hadoop.ozone.container.keyvalue.scanner;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto.Reason.CORRUPT_CHUNK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto.Reason.INCONSISTENT_CHUNK_LENGTH;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto.Reason.MISSING_CHUNK_FILE;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto.Reason;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies a single block of a container: presence of the chunk files and their checksums.
 * Used by the VerifyBlock datanode command (ozone admin fscheck).
 */
public class BlockScanner {
  private static final Logger LOG = LoggerFactory.getLogger(BlockScanner.class);
  private static final DirectBufferPool BUFFER_POOL = new DirectBufferPool();

  private final KeyValueContainerData onDiskContainerData;

  public BlockScanner(KeyValueContainerData onDiskContainerData) {
    this.onDiskContainerData = onDiskContainerData;
  }

  /**
   * Scans the given block for missing or corrupted chunk files and verifies their checksums.
   *
   * @param block the block to be scanned
   * @return {@code null} if the block is healthy, otherwise the reason of the first encountered issue
   */
  public Reason scanBlock(BlockData block) {
    ContainerLayoutVersion layout = onDiskContainerData.getLayoutVersion();

    boolean isFirstChunkNotEmpty = !block.getChunks().isEmpty() && block.getChunks().get(0).getLen() > 0;

    for (ContainerProtos.ChunkInfo chunk : block.getChunks()) {
      Path chunkFile;
      try {
        chunkFile = layout.getChunkFile(onDiskContainerData, block.getBlockID(), chunk.getChunkName()).toPath();
      } catch (IOException ex) {
        LOG.warn("Unable to resolve chunk file of block {}", block.getBlockID(), ex);
        return MISSING_CHUNK_FILE;
      }

      if (!Files.exists(chunkFile)) {
        // In EC, a client may write empty putBlock in padding block nodes.
        // So, we need to make sure, chunk length > 0, before declaring the missing chunk file.
        if (isFirstChunkNotEmpty) {
          LOG.warn("Missing chunk file {} of block {}", chunkFile, block.getBlockID());
          return MISSING_CHUNK_FILE;
        }
      } else if (chunk.getChecksumData().getType() != ContainerProtos.ChecksumType.NONE) {
        Reason result = verifyChecksum(block, chunk, chunkFile, layout);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }

  private Reason verifyChecksum(BlockData block, ContainerProtos.ChunkInfo chunk, Path chunkFile,
      ContainerLayoutVersion layout) {
    ChecksumData checksumData = ChecksumData.getFromProtoBuf(chunk.getChecksumData());
    int checksumCount = checksumData.getChecksums().size();
    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    long bytesRead = 0;

    ByteBuffer buffer = BUFFER_POOL.getBuffer(bytesPerChecksum);
    Checksum cal = new Checksum(checksumData.getChecksumType(), bytesPerChecksum);

    try (FileChannel channel = FileChannel.open(chunkFile, ChunkUtils.READ_OPTIONS, ChunkUtils.NO_ATTRIBUTES)) {
      if (layout == FILE_PER_BLOCK) {
        channel.position(chunk.getOffset());
      }
      for (int i = 0; i < checksumCount; i++) {
        // Limit last read for FILE_PER_BLOCK, to avoid reading the next chunk
        if (layout == FILE_PER_BLOCK && i == checksumCount - 1 && chunk.getLen() % bytesPerChecksum != 0) {
          buffer.limit((int) (chunk.getLen() % bytesPerChecksum));
        }

        int v = channel.read(buffer);
        if (v == -1) {
          break;
        }
        bytesRead += v;
        buffer.flip();

        ByteString expected = checksumData.getChecksums().get(i);
        ByteString actual = cal.computeChecksum(buffer).getChecksums().get(0);
        if (!expected.equals(actual)) {
          LOG.warn("Inconsistent read for chunk {} checksum item {} of block {}",
              chunk.getChunkName(), i, block.getBlockID());
          return CORRUPT_CHUNK;
        }
        buffer.clear();
      }
      if (bytesRead != chunk.getLen()) {
        LOG.warn("Inconsistent read for chunk {} expected length={} actual length={} of block {}",
            chunk.getChunkName(), chunk.getLen(), bytesRead, block.getBlockID());
        return INCONSISTENT_CHUNK_LENGTH;
      }
    } catch (IOException ex) {
      LOG.warn("Failed to read chunk file {} of block {}", chunkFile, block.getBlockID(), ex);
      return MISSING_CHUNK_FILE;
    } finally {
      buffer.clear();
      BUFFER_POOL.returnBuffer(buffer);
    }
    return null;
  }
}
