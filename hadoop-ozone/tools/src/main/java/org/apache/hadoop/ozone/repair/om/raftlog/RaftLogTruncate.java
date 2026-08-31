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

package org.apache.hadoop.ozone.repair.om.raftlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import picocli.CommandLine;

/**
 * {@code ozone repair om raft-log truncate}: drop every raft-log entry whose
 * index exceeds {@code --index N}. Operates at segment-file granularity:
 *
 * <ul>
 *   <li>Segments whose {@code startIndex > N} are deleted.</li>
 *   <li>The segment containing {@code N} is rewritten to keep entries
 *       {@code [startIndex, N]}; if {@code N == endIndex} the segment is
 *       untouched.</li>
 *   <li>If {@code N} is not present in any segment the command refuses.</li>
 * </ul>
 *
 * After truncate, restart the OM and let install-snapshot from the leader
 * repopulate the missing tail. The OM must NOT be running against this
 * raft-log dir while truncate runs.
 */
@CommandLine.Command(name = "truncate",
    description = "Truncate the raft log to the given (last good) index, "
        + "deleting newer segments. The OM must not be running.")
public class RaftLogTruncate implements Callable<Void> {

  private static final int DEFAULT_SEGMENT_SIZE = 4 * 1024 * 1024;
  private static final int DEFAULT_PREALLOCATED_SIZE = 4 * 1024 * 1024;
  private static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;

  @CommandLine.ParentCommand
  private RaftLogRepair parent;

  @CommandLine.Option(names = {"--index"},
      required = true,
      description = "Last good raft-log index to keep. Entries with index > N "
          + "are removed.")
  private long targetIndex;

  @CommandLine.Option(names = {"--dry-run"},
      description = "Show planned changes without modifying any files.",
      defaultValue = "true")
  private boolean dryRun;

  @Override
  public Void call() throws Exception {
    File root = new File(parent.getRaftLogDir());
    if (!root.isDirectory()) {
      System.err.println("raft-log-dir does not exist or is not a directory: "
          + root);
      return null;
    }
    System.out.println("Make sure the OM is not running against this raft-log "
        + "dir. dry-run=" + dryRun + ", target index=" + targetIndex);

    Map<Path, List<LogSegmentPath>> byDir =
        RaftLogInspect.collectSegments(root);
    if (byDir.isEmpty()) {
      System.out.println("No raft-log segments found under " + root);
      return null;
    }
    for (Map.Entry<Path, List<LogSegmentPath>> entry : byDir.entrySet()) {
      truncateOneGroupDir(entry.getKey(), entry.getValue());
    }
    return null;
  }

  private void truncateOneGroupDir(Path groupDir, List<LogSegmentPath> segments)
      throws Exception {
    System.out.println();
    System.out.println("=== " + groupDir + " ===");
    segments.sort(Comparator.comparingLong(s -> s.getStartEnd().getStartIndex()));

    List<LogSegmentPath> toDelete = new ArrayList<>();
    LogSegmentPath toRewrite = null;
    boolean targetWithinRange = false;
    for (LogSegmentPath seg : segments) {
      long start = seg.getStartEnd().getStartIndex();
      boolean open = seg.getStartEnd().isOpen();
      long end = open ? Long.MAX_VALUE : seg.getStartEnd().getEndIndex();
      if (start > targetIndex) {
        toDelete.add(seg);
      } else if (targetIndex >= start && targetIndex <= end) {
        toRewrite = seg;
        targetWithinRange = true;
      }
    }

    if (!targetWithinRange) {
      System.err.println("  refusing: target index " + targetIndex + " is not "
          + "in any segment under " + groupDir + ". Run 'inspect' to pick an "
          + "index that exists.");
      return;
    }

    for (LogSegmentPath seg : toDelete) {
      File f = seg.getPath().toFile();
      if (dryRun) {
        System.out.println("  rm " + f);
      } else {
        if (f.delete()) {
          System.out.println("  deleted " + f);
        } else {
          System.err.println("  failed to delete " + f);
        }
      }
    }

    long rewriteEnd = toRewrite.getStartEnd().isOpen()
        ? Long.MAX_VALUE : toRewrite.getStartEnd().getEndIndex();
    if (!toRewrite.getStartEnd().isOpen() && rewriteEnd == targetIndex) {
      System.out.println("  segment " + toRewrite.getPath().getFileName()
          + " already ends at target index " + targetIndex + "; no rewrite "
          + "needed.");
      return;
    }

    long startIdx = toRewrite.getStartEnd().getStartIndex();
    File src = toRewrite.getPath().toFile();
    File closedReplacement = new File(groupDir.toFile(),
        String.format("log_%d-%d", startIdx, targetIndex));
    if (dryRun) {
      System.out.println("  rewrite " + src.getName() + " -> "
          + closedReplacement.getName() + " (keep entries ["
          + startIdx + ", " + targetIndex + "])");
      return;
    }
    rewriteSegment(src, closedReplacement, startIdx, rewriteEnd,
        toRewrite.getStartEnd().isOpen(), targetIndex);
  }

  private void rewriteSegment(File src, File dst, long declaredStart,
      long declaredEnd, boolean isOpen, long lastGoodIndex) throws Exception {
    File tmp = new File(dst.getParentFile(), dst.getName() + ".tmp");
    if (tmp.exists() && !tmp.delete()) {
      throw new IOException("Failed to clear pre-existing tmp file " + tmp);
    }
    Object reader = null;
    try {
      reader = RaftLogInspect.openInputStream(src, declaredStart, declaredEnd,
          isOpen);
      ByteBuffer buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
      long lastWritten = -1L;
      try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(
          tmp, false, DEFAULT_SEGMENT_SIZE, DEFAULT_PREALLOCATED_SIZE, buf)) {
        while (true) {
          LogEntryProto entry = RaftLogInspect.nextEntry(reader);
          if (entry == null) {
            break;
          }
          if (entry.getIndex() > lastGoodIndex) {
            break;
          }
          out.write(entry);
          lastWritten = entry.getIndex();
        }
        out.flush();
      }
      if (lastWritten != lastGoodIndex) {
        if (!tmp.delete()) {
          System.err.println("    failed to delete tmp " + tmp);
        }
        throw new IOException("Refusing to commit truncation of " + src
            + ": last entry actually present is index " + lastWritten
            + ", not requested " + lastGoodIndex);
      }
      Files.move(tmp.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      if (!src.getAbsolutePath().equals(dst.getAbsolutePath())) {
        if (src.exists() && !src.delete()) {
          System.err.println("    failed to delete original segment " + src);
        }
      }
      System.out.println("  rewrote " + src.getName() + " -> " + dst.getName()
          + " (kept [" + declaredStart + ", " + lastGoodIndex + "])");
    } finally {
      if (reader != null) {
        try {
          RaftLogInspect.closeInputStream(reader);
        } catch (Exception ignored) {
          // best-effort close
        }
      }
    }
  }
}
