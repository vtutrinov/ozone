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

package org.apache.hadoop.ozone.container.keyvalue.scanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import static org.apache.hadoop.hdds.StringUtils.bytes2Hex;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult.FailureType.CORRUPT_CHUNK;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult.FailureType.INCONSISTENT_CHUNK_LENGTH;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult.FailureType.MISSING_CHUNK_FILE;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult.unhealthy;

/**
 * The BlockScanner class is responsible for scanning and verifying blocks of data stored in a container,
 * ensuring data integrity and correctness.
 * The scanner can perform checksum verification to detect any corrupted chunk data.
 * It leverages a buffer pool and can be throttled by a {@link DataTransferThrottler}.
 */
public class BlockScanner {
  private static final DirectBufferPool BUFFER_POOL = new DirectBufferPool();

  private final KeyValueContainerData onDiskContainerData;

  private @Nullable final DataTransferThrottler throttler;

  private @Nullable final Canceler canceler;

  public BlockScanner(KeyValueContainerData onDiskContainerData) {
    this(onDiskContainerData, null, null);
  }

  public BlockScanner(KeyValueContainerData onDiskContainerData, @Nullable DataTransferThrottler throttler,
      @Nullable Canceler canceler) {
    this.onDiskContainerData = onDiskContainerData;
    this.throttler = throttler;
    this.canceler = canceler;
  }

  /**
   * Scans the given block for missing or corrupted chunk files and verifies their checksums.
   *
   * @param block The {@link BlockData} object representing the block to be scanned.
   * @return A {@link ScanResult} object indicating whether the block is healthy or, if not,
   *         providing details about the first encountered issue.
   */
  public ScanResult scanBlock(BlockData block) {
    ContainerLayoutVersion layout = onDiskContainerData.getLayoutVersion();

    boolean isFirstChunkNotEmpty = block.getChunks().get(0).getLen() > 0;

    for (ContainerProtos.ChunkInfo chunk : block.getChunks()) {
      Path chunkFile;
      try {
        chunkFile = layout.getChunkFile(onDiskContainerData, block.getBlockID(), chunk.getChunkName()).toPath();
      } catch (IOException ex) {
        return unhealthy(MISSING_CHUNK_FILE, Paths.get(onDiskContainerData.getChunksPath()), ex);
      }

      if (!Files.exists(chunkFile)) {
        // In EC, a client may write empty putBlock in padding block nodes.
        // So, we need to make sure, chunk length > 0, before declaring the missing chunk file.
        if (isFirstChunkNotEmpty) {
          return unhealthy(MISSING_CHUNK_FILE, chunkFile,
            new IOException("Missing chunk file " + chunkFile.toAbsolutePath()));
        }
      } else if (chunk.getChecksumData().getType() != ContainerProtos.ChecksumType.NONE) {
        ScanResult result = verifyChecksum(block, chunk, chunkFile, layout);

        if (!result.isHealthy()) {
          return result;
        }
      }
    }

    return ScanResult.healthy();
  }

  private ScanResult verifyChecksum(BlockData block, ContainerProtos.ChunkInfo chunk, Path chunkFile,
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

        if (throttler != null) {
          throttler.throttle(v, Objects.requireNonNull(canceler));
        }

        ByteString expected = checksumData.getChecksums().get(i);
        ByteString actual = cal.computeChecksum(buffer).getChecksums().get(0);
        if (!expected.equals(actual)) {
          String message = String.format("Inconsistent read for chunk=%s checksum item %d "
                  + "expected checksum %s actual checksum %s for block %s",
              ChunkInfo.getFromProtoBuf(chunk),
              i,
              bytes2Hex(expected.asReadOnlyByteBuffer()),
              bytes2Hex(actual.asReadOnlyByteBuffer()),
              block.getBlockID()
          );
          return ScanResult.unhealthy(CORRUPT_CHUNK, chunkFile, new IOException(message));
        }
      }
      if (bytesRead != chunk.getLen()) {
        String message =
            String.format("Inconsistent read for chunk=%s expected length=%d actual length=%d for block %s",
                chunk.getChunkName(),
                chunk.getLen(),
                bytesRead,
                block.getBlockID()
            );
        return ScanResult.unhealthy(INCONSISTENT_CHUNK_LENGTH, chunkFile, new IOException(message));
      }
    } catch (IOException ex) {
      return ScanResult.unhealthy(MISSING_CHUNK_FILE, chunkFile, ex);
    } finally {
      buffer.clear();
      BUFFER_POOL.returnBuffer(buffer);
    }

    return ScanResult.healthy();
  }
}
