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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import picocli.CommandLine;

/**
 * {@code ozone repair om raft-log inspect}: walk each segment file under the
 * given raft-log dir and report (a) per-segment {@code (startIndex, endIndex,
 * term)} metadata derived from filename + first entry, (b) inter-segment gaps,
 * and (c) intra-segment gaps — the failure mode behind the HDDS-15068 crash
 * where consecutive entries in one segment file are not at adjacent indices.
 */
@CommandLine.Command(name = "inspect",
    description = "List raft-log segments and detect inter- and intra-segment gaps.")
public class RaftLogInspect implements Callable<Void> {

  @CommandLine.ParentCommand
  private RaftLogRepair parent;

  private int intraSegmentGaps;

  @Override
  public Void call() throws Exception {
    File root = new File(parent.getRaftLogDir());
    if (!root.isDirectory()) {
      System.err.println("raft-log-dir does not exist or is not a directory: " + root);
      return null;
    }
    System.out.println(
        "Make sure the OM is not running against this raft-log dir.");
    Map<Path, List<LogSegmentPath>> byDir = collectSegments(root);
    if (byDir.isEmpty()) {
      System.out.println("No raft-log segments found under " + root);
      return null;
    }
    int totalGaps = 0;
    for (Map.Entry<Path, List<LogSegmentPath>> entry : byDir.entrySet()) {
      System.out.println();
      System.out.println("=== " + entry.getKey() + " ===");
      List<LogSegmentPath> segments = entry.getValue();
      segments.sort(Comparator.comparingLong(s -> s.getStartEnd().getStartIndex()));
      totalGaps += reportSegments(segments);
    }
    totalGaps += intraSegmentGaps;
    System.out.println();
    System.out.println(totalGaps == 0
        ? "No gaps detected."
        : "Detected " + totalGaps + " gap(s). Use 'ozone repair om raft-log "
            + "truncate --raft-log-dir " + root + " --index <last-good-index>' "
            + "to recover; restart the OM and let install-snapshot from the "
            + "leader repopulate.");
    return null;
  }

  private int reportSegments(List<LogSegmentPath> segments) {
    int gaps = 0;
    long expectedNextStart = -1L;
    for (LogSegmentPath seg : segments) {
      long start = seg.getStartEnd().getStartIndex();
      boolean open = seg.getStartEnd().isOpen();
      long end = open ? -1L : seg.getStartEnd().getEndIndex();
      System.out.printf("  segment %-40s start=%d end=%s open=%b%n",
          seg.getPath().getFileName(), start, open ? "?" : String.valueOf(end),
          open);
      if (expectedNextStart >= 0 && start != expectedNextStart) {
        gaps++;
        System.out.printf("    ! inter-segment gap: previous segment ended at %d,"
            + " this one starts at %d (expected %d)%n",
            expectedNextStart - 1, start, expectedNextStart);
      }
      long lastIndexInSegment = inspectEntries(seg.getPath().toFile(), start,
          end, open);
      if (open) {
        // For open segments the end isn't known from the filename; use the
        // last index we actually read. If we couldn't read entries, skip the
        // inter-segment gap check for the next iteration.
        expectedNextStart = lastIndexInSegment >= 0
            ? lastIndexInSegment + 1 : -1L;
      } else {
        expectedNextStart = end + 1;
      }
    }
    return gaps;
  }

  /**
   * Walk the entries of a single segment file using the (package-private)
   * {@code SegmentedRaftLogInputStream}. Returns the last index successfully
   * read, or -1 if the file could not be opened. Prints each intra-segment
   * gap with the surrounding {@code (term, index)} pair.
   *
   * Uses reflection because Ratis 3.0.1 doesn't expose a public reader; if
   * Ratis is upgraded and the API breaks, this method falls back to filename-
   * only inspection so the caller still gets useful output.
   */
  private long inspectEntries(File file, long declaredStart, long declaredEnd,
      boolean isOpen) {
    Object stream;
    try {
      stream = openInputStream(file, declaredStart, declaredEnd, isOpen);
    } catch (ReflectiveOperationException | RuntimeException e) {
      System.out.println("    (skipped per-entry inspection: "
          + e.getClass().getSimpleName() + ": " + e.getMessage() + ")");
      return -1L;
    }
    long prevIndex = -1L;
    long prevTerm = -1L;
    long count = 0;
    try {
      while (true) {
        LogEntryProto entry = nextEntry(stream);
        if (entry == null) {
          break;
        }
        long idx = entry.getIndex();
        long term = entry.getTerm();
        if (prevIndex >= 0 && idx != prevIndex + 1) {
          intraSegmentGaps++;
          System.out.printf("    ! intra-segment gap: term=%d index=%d -> "
              + "term=%d index=%d (missing %d entr%s)%n",
              prevTerm, prevIndex, term, idx, idx - prevIndex - 1,
              idx - prevIndex - 1 == 1 ? "y" : "ies");
        }
        prevIndex = idx;
        prevTerm = term;
        count++;
      }
    } catch (Exception e) {
      System.out.println("    (entry walk aborted: "
          + e.getClass().getSimpleName() + ": " + e.getMessage() + ")");
    } finally {
      try {
        closeInputStream(stream);
      } catch (Exception ignored) {
        // best-effort close
      }
    }
    if (count == 0) {
      System.out.println("    (segment contained no entries)");
    } else {
      System.out.printf("    walked %d entr%s, last index=%d term=%d%n",
          count, count == 1 ? "y" : "ies", prevIndex, prevTerm);
    }
    return prevIndex;
  }

  static Map<Path, List<LogSegmentPath>> collectSegments(File root)
      throws IOException {
    Map<Path, List<LogSegmentPath>> byDir = new LinkedHashMap<>();
    try (Stream<Path> stream = Files.walk(root.toPath())) {
      stream.filter(Files::isRegularFile).forEach(p -> {
        LogSegmentPath seg = LogSegmentPath.matchLogSegment(p);
        if (seg != null) {
          byDir.computeIfAbsent(p.getParent(), k -> new ArrayList<>()).add(seg);
        }
      });
    }
    return byDir;
  }

  static Object openInputStream(File file, long startIndex, long endIndex,
      boolean isOpen) throws ReflectiveOperationException {
    Class<?> streamClass = Class.forName(
        "org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream");
    Class<?> sizeClass = Class.forName("org.apache.ratis.util.SizeInBytes");
    Object maxOpSize = sizeClass
        .getMethod("valueOf", String.class).invoke(null, "32MB");
    java.lang.reflect.Constructor<?> ctor = streamClass.getDeclaredConstructor(
        File.class, long.class, long.class, boolean.class, sizeClass,
        Class.forName(
            "org.apache.ratis.server.metrics.SegmentedRaftLogMetrics"));
    ctor.setAccessible(true);
    return ctor.newInstance(file, startIndex, isOpen ? -1L : endIndex, isOpen,
        maxOpSize, null);
  }

  static LogEntryProto nextEntry(Object stream)
      throws ReflectiveOperationException {
    Object res = stream.getClass().getMethod("nextEntry").invoke(stream);
    return (LogEntryProto) res;
  }

  static void closeInputStream(Object stream)
      throws ReflectiveOperationException {
    stream.getClass().getMethod("close").invoke(stream);
  }
}
