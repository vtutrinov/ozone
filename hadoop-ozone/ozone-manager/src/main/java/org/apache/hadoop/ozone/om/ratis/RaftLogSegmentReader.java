/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.util.SizeInBytes;

/**
 * Reads the entries of one Ratis raft log segment file one by one, without
 * the index contiguity assertion that {@code LogSegment.readSegmentFile}
 * applies. That makes it usable on segments that are already damaged, e.g.
 * to find the highest index present in a file with an intra-segment gap.
 *
 * Ratis 3.0.1 exposes {@link SegmentedRaftLogInputStream} publicly but keeps
 * its constructor package-private, hence the reflective construction.
 */
public final class RaftLogSegmentReader implements Closeable {

  private final SegmentedRaftLogInputStream in;

  private RaftLogSegmentReader(SegmentedRaftLogInputStream in) {
    this.in = in;
  }

  /**
   * @param endIndex ignored for an open segment
   */
  public static RaftLogSegmentReader open(File file, long startIndex, long endIndex,
      boolean isOpen, SizeInBytes maxOpSize) throws IOException {
    try {
      final Constructor<SegmentedRaftLogInputStream> ctor =
          SegmentedRaftLogInputStream.class.getDeclaredConstructor(File.class, long.class,
              long.class, boolean.class, SizeInBytes.class, SegmentedRaftLogMetrics.class);
      ctor.setAccessible(true);
      return new RaftLogSegmentReader(
          ctor.newInstance(file, startIndex, isOpen ? -1L : endIndex, isOpen, maxOpSize, null));
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new IOException("Failed to open raft log segment " + file, e);
    }
  }

  /** @return the next entry, or null at the end of the segment. */
  public LogEntryProto nextEntry() throws IOException {
    return in.nextEntry();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
