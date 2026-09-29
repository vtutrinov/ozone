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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMRaftLogInconsistencyException;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.LogSegmentStartEnd;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-flight consistency checks between the OM DB (the state machine snapshot,
 * i.e. the persisted {@code TransactionInfo}) and the Ratis segmented raft log
 * of one raft group. Must run from {@code StateMachine.initialize()}, which
 * Ratis calls before it opens the raft log, so the segment files can still be
 * removed safely.
 *
 * <p>Root cause this protects against (HDDS-15068 / HDDS-15103 / HDDS-15133,
 * Ratis 3.0.1): when the OM dies between "DB replaced by the leader's
 * checkpoint at index S" and "raft log purged", it restarts with the DB ahead
 * of its raft log (S &gt; E, E being the last index in the log). Ratis then
 * purges the closed segments but keeps the stale <em>open</em> segment and
 * re-opens its file for append with {@code lastWrittenIndex = S}. The next
 * leader append at S+1 is written into that file right after E before the
 * in-memory gap check fires, leaving a permanently corrupt segment
 * ("gap between entry ... and entry ..." on every following startup).
 *
 * <p>Two repairs are applied automatically, both resting on the same
 * argument: an entry whose index is not above the DB index is already part of
 * the state machine snapshot, so removing it from the raft log loses nothing
 * (it is what a Ratis purge does).
 * <ul>
 *   <li>Healthy but stale log (S &gt; E): all segments are deleted and Ratis
 *       starts with an empty log; the leader's first append creates a fresh
 *       segment at S+1. This prevents the corruption.</li>
 *   <li>Inconsistent log (inter-segment gap, segment with a gap or damaged
 *       bytes) whose highest index on disk T is not above S: all segments are
 *       deleted for the same reason. This recovers a log that is already
 *       corrupt without operator involvement.</li>
 * </ul>
 * An inconsistent log that holds entries above the DB index (T &gt; S), or
 * whose content cannot be read to the end, is never touched: those entries
 * were acknowledged to the leader but are not applied, and dropping them is
 * an operator decision ({@code ozone repair om raft-log truncate}). The OM
 * then refuses to start, unless the fail-fast is switched off.
 */
public final class OMRaftLogAligner {

  public static final Logger LOG = LoggerFactory.getLogger(OMRaftLogAligner.class);

  /** Highest index on disk could not be determined (unreadable segment). */
  private static final long UNKNOWN_INDEX = Long.MAX_VALUE;

  private OMRaftLogAligner() {
  }

  public static boolean isEnabled(ConfigurationSource conf) {
    return conf.getBoolean(OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED,
        OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED_DEFAULT);
  }

  /**
   * Runs the full pre-flight: apply the automatic repairs, then fail fast on
   * an inconsistent log that could not be repaired. The repairs always run;
   * the config switch only gates the fail-fast so that an operator can bypass
   * it during a manual recovery without losing the prevention.
   *
   * @param raftStorage        raft storage of the group being initialized
   * @param lastAppliedFromDb  TransactionInfo term/index loaded from the OM DB
   *                           for this group (may be null on a fresh OM)
   * @param properties         raft properties of the server (for the entry
   *                           size limit used when reading the open segment)
   * @param conf               OM configuration (fail-fast switch)
   * @param groupLabel         label used in log messages
   */
  public static void preflight(RaftStorage raftStorage, TermIndex lastAppliedFromDb,
      RaftProperties properties, ConfigurationSource conf, String groupLabel)
      throws IOException {
    final SizeInBytes maxOpSize =
        RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
    final LogState state = inspect(raftStorage, lastAppliedFromDb, maxOpSize);
    if (repair(state, groupLabel) > 0 || state.problem == null) {
      return;
    }
    if (!isEnabled(conf)) {
      LOG.warn("{}: {} Not failing because {} is false; Ratis will report the problem itself.",
          groupLabel, state.describeUnrepaired(), OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED);
      return;
    }
    throw state.toException(groupLabel);
  }

  /**
   * Applies the automatic repairs described in the class comment.
   *
   * @return number of segment files deleted; 0 when nothing had to be (or
   *         could safely be) removed.
   */
  public static int alignStaleRaftLog(RaftStorage raftStorage, TermIndex lastAppliedFromDb,
      SizeInBytes maxOpSize, String groupLabel) throws IOException {
    return repair(inspect(raftStorage, lastAppliedFromDb, maxOpSize), groupLabel);
  }

  /**
   * Fails with {@link OMRaftLogInconsistencyException} when the segment files
   * of the group do not form a readable, contiguous index range, so the
   * operator gets an actionable message pointing at
   * {@code ozone repair om raft-log} instead of an opaque Ratis exception.
   */
  public static void failOnInconsistentLog(RaftStorage raftStorage, TermIndex lastAppliedFromDb,
      SizeInBytes maxOpSize, String groupLabel) throws IOException {
    final LogState state = inspect(raftStorage, lastAppliedFromDb, maxOpSize);
    if (state.problem != null) {
      throw state.toException(groupLabel);
    }
  }

  private static int repair(LogState state, String groupLabel) throws IOException {
    if (state.segments.isEmpty() || state.dbIndex < 0) {
      return 0;
    }
    if (state.problem == null) {
      if (state.dbIndex <= state.endIndex) {
        final long logStartIndex = state.segments.get(0).getStartEnd().getStartIndex();
        if (state.dbIndex + 1 < logStartIndex) {
          LOG.warn("{}: OM DB is at index {} but the raft log starts at index {}; entries {}..{} "
                  + "are not available locally and must come from the leader.",
              groupLabel, state.dbIndex, logStartIndex, state.dbIndex + 1, logStartIndex - 1);
        }
        LOG.debug("{}: raft log end index {} covers OM DB index {}; nothing to align.",
            groupLabel, state.endIndex, state.dbIndex);
        return 0;
      }
      final int deleted = deleteAll(state.segments);
      LOG.warn("{}: OM DB index {} is past the raft log end index {} (DB was replaced by a "
              + "snapshot but the raft log was not purged, see HDDS-15068). All {} raft log "
              + "segment file(s) only hold entries already included in the snapshot and were "
              + "deleted. Ratis will start with an empty log and continue from index {}.",
          groupLabel, state.dbIndex, state.endIndex, deleted, state.dbIndex + 1);
      return deleted;
    }

    if (state.maxIndexOnDisk == UNKNOWN_INDEX || state.dbIndex < state.maxIndexOnDisk) {
      return 0;
    }
    final int deleted = deleteAll(state.segments);
    LOG.warn("{}: OM Ratis raft log in {} is inconsistent ({}), but the OM DB index {} covers "
            + "every entry on disk (highest index {}). All {} raft log segment file(s) only hold "
            + "entries already applied to the DB and were deleted. Ratis will start with an "
            + "empty log and continue from index {}.",
        groupLabel, state.dir, state.problem, state.dbIndex, state.maxIndexOnDisk, deleted,
        state.dbIndex + 1);
    return deleted;
  }

  private static int deleteAll(List<LogSegmentPath> segments) throws IOException {
    int deleted = 0;
    for (LogSegmentPath segment : segments) {
      Files.delete(segment.getPath());
      deleted++;
    }
    return deleted;
  }

  /**
   * Looks at the segment files of the group. Closed segments are judged by
   * their file names only; the open segment is read when it is the one thing
   * that could still cover the DB index. Nothing else is read unless a
   * problem was found, so a healthy log costs no additional I/O.
   */
  private static LogState inspect(RaftStorage raftStorage, TermIndex lastAppliedFromDb,
      SizeInBytes maxOpSize) throws IOException {
    final LogState state = new LogState(
        LogSegmentPath.getLogSegmentPaths(raftStorage),
        raftStorage.getStorageDir().getCurrentDir(),
        lastAppliedFromDb == null ? RaftLog.INVALID_LOG_INDEX : lastAppliedFromDb.getIndex());

    LogSegmentPath openSegment = null;
    for (int i = 0; i < state.segments.size(); i++) {
      final LogSegmentPath curr = state.segments.get(i);
      final LogSegmentStartEnd currRange = curr.getStartEnd();
      if (i > 0 && state.problem == null) {
        final LogSegmentPath prev = state.segments.get(i - 1);
        final LogSegmentStartEnd prevRange = prev.getStartEnd();
        if (prevRange.isOpen()) {
          // An in-progress segment must be the last segment in the dir.
          state.problem = "corrupted segment layout: in-progress segment " + fileName(prev)
              + " is followed by " + fileName(curr);
        } else if (currRange.getStartIndex() != prevRange.getEndIndex() + 1) {
          state.problem = "inter-segment gap: segment " + fileName(prev) + " ends at index "
              + prevRange.getEndIndex() + " but next segment " + fileName(curr)
              + " starts at index " + currRange.getStartIndex() + " (expected "
              + (prevRange.getEndIndex() + 1) + ")";
          state.lastGoodIndex = prevRange.getEndIndex();
        }
      }
      if (currRange.isOpen()) {
        openSegment = curr;
      } else if (state.problem == null) {
        state.endIndex = currRange.getEndIndex();
      }
    }

    if (state.problem == null && openSegment != null && state.dbIndex > state.endIndex) {
      final LogSegmentStartEnd range = openSegment.getStartEnd();
      if (state.dbIndex < range.getStartIndex()) {
        // The open segment starts past the DB index, so the DB index is
        // covered whatever the segment holds (empty counts as start - 1).
        state.endIndex = range.getStartIndex() - 1;
      } else {
        try {
          final int entries = LogSegment.readSegmentFile(openSegment.getPath().toFile(), range,
              maxOpSize, raftStorage.getLogCorruptionPolicy(), null, null);
          state.endIndex = range.getStartIndex() + entries - 1;
        } catch (IllegalStateException | IOException e) {
          // readSegmentFile asserts index contiguity (IllegalStateException)
          // and fails on checksum/header/size errors (IOException).
          state.problem = "segment " + fileName(openSegment) + " is corrupt ("
              + String.valueOf(e.getMessage()).replaceAll("\\s+", " ").trim() + ")";
        }
      }
    }

    if (state.problem != null) {
      state.maxIndexOnDisk = findMaxIndexOnDisk(state.segments, maxOpSize);
    }
    return state;
  }

  /**
   * Highest entry index present in any segment file, gaps or not: closed
   * segments by their file names, open segments by walking their entries
   * without the contiguity assertion. {@link #UNKNOWN_INDEX} when an open
   * segment cannot be read to its end.
   */
  private static long findMaxIndexOnDisk(List<LogSegmentPath> segments, SizeInBytes maxOpSize) {
    long max = RaftLog.INVALID_LOG_INDEX;
    for (LogSegmentPath segment : segments) {
      final LogSegmentStartEnd range = segment.getStartEnd();
      if (!range.isOpen()) {
        max = Math.max(max, range.getEndIndex());
        continue;
      }
      try (RaftLogSegmentReader reader = RaftLogSegmentReader.open(segment.getPath().toFile(),
          range.getStartIndex(), range.getEndIndex(), true, maxOpSize)) {
        for (LogEntryProto entry = reader.nextEntry(); entry != null; entry = reader.nextEntry()) {
          max = Math.max(max, entry.getIndex());
        }
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to read raft log segment {} to its end; its content is unknown.",
            segment.getPath(), e);
        return UNKNOWN_INDEX;
      }
    }
    return max;
  }

  private static String fileName(LogSegmentPath segment) {
    return segment.getPath().getFileName().toString();
  }

  /** What {@link #inspect} found. */
  private static final class LogState {
    private final List<LogSegmentPath> segments;
    private final File dir;
    private final long dbIndex;
    /** Last index of the contiguous, readable log; meaningful when problem is null. */
    private long endIndex = RaftLog.INVALID_LOG_INDEX;
    /** Description of the inconsistency, null for a healthy log. */
    private String problem;
    /** Last index before the inconsistency, when known from the file names. */
    private long lastGoodIndex = RaftLog.INVALID_LOG_INDEX;
    /** Highest index on disk; computed only for an inconsistent log. */
    private long maxIndexOnDisk = RaftLog.INVALID_LOG_INDEX;

    private LogState(List<LogSegmentPath> segments, File dir, long dbIndex) {
      this.segments = segments;
      this.dir = dir;
      this.dbIndex = dbIndex;
    }

    private String describeUnrepaired() {
      final String coverage = maxIndexOnDisk == UNKNOWN_INDEX
          ? "its content cannot be read to the end, so it is unknown whether the OM DB (index "
              + dbIndex + ") covers it"
          : "it holds entries up to index " + maxIndexOnDisk + " while the OM DB is at index "
              + dbIndex + ", so entries " + (dbIndex + 1) + ".." + maxIndexOnDisk
              + " are not applied and would be lost by discarding the log";
      return "OM Ratis raft log in " + dir + " is inconsistent (" + problem + ") and was not "
          + "repaired automatically: " + coverage + ".";
    }

    private OMRaftLogInconsistencyException toException(String groupLabel) {
      final String index = lastGoodIndex >= 0 ? String.valueOf(lastGoodIndex) : "<last-good-index>";
      return new OMRaftLogInconsistencyException(groupLabel + ": " + describeUnrepaired()
          + " Run 'ozone repair om raft-log inspect --raft-log-dir " + dir
          + "' to diagnose, then 'ozone repair om raft-log truncate --raft-log-dir " + dir
          + " --index " + index + "' to recover.");
    }
  }
}
