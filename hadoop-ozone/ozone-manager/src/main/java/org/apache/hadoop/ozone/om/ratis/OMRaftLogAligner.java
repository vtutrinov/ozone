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
 * moved around safely.
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
 * <p>Fix: when S &gt; E every local segment only holds entries that are already
 * part of the installed snapshot, so they are deleted (exactly what a Ratis
 * purge would do) and Ratis starts with an empty log; the leader's first
 * append creates a fresh segment starting at S+1.
 */
public final class OMRaftLogAligner {

  public static final Logger LOG = LoggerFactory.getLogger(OMRaftLogAligner.class);

  private OMRaftLogAligner() {
  }

  public static boolean isEnabled(ConfigurationSource conf) {
    return conf.getBoolean(OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED,
        OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED_DEFAULT);
  }

  /**
   * Runs the full pre-flight: align a stale raft log with the DB snapshot,
   * then fail fast on an inter-segment gap in whatever is left. The alignment
   * is the actual corruption prevention and always runs (it makes the same
   * decision Ratis makes in {@code loadLogSegments}, only completely); the
   * config switch only gates the fail-fast diagnostics so that an operator can
   * bypass them during a manual recovery without losing the prevention.
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
    alignStaleRaftLog(raftStorage, lastAppliedFromDb, maxOpSize, groupLabel);
    if (!isEnabled(conf)) {
      LOG.info("{}: raft log gap fail-fast disabled by {}", groupLabel,
          OMConfigKeys.OZONE_OM_RATIS_LOG_GAP_CHECK_ENABLED);
      return;
    }
    failOnInterSegmentGap(raftStorage, groupLabel);
  }

  /**
   * If the DB snapshot index is past the last index present in the raft log,
   * delete every segment file: all of them only hold entries that are already
   * included in the installed snapshot.
   *
   * @return number of segment files deleted; 0 when the log is consistent
   *         with the DB.
   */
  public static int alignStaleRaftLog(RaftStorage raftStorage, TermIndex lastAppliedFromDb,
      SizeInBytes maxOpSize, String groupLabel) throws IOException {
    final long dbIndex = lastAppliedFromDb == null
        ? RaftLog.INVALID_LOG_INDEX : lastAppliedFromDb.getIndex();
    final List<LogSegmentPath> segments = LogSegmentPath.getLogSegmentPaths(raftStorage);
    if (segments.isEmpty() || dbIndex < 0) {
      return 0;
    }

    final long logEndIndex = computeLogEndIndex(raftStorage, segments, maxOpSize, dbIndex, groupLabel);
    if (dbIndex <= logEndIndex) {
      final long logStartIndex = segments.get(0).getStartEnd().getStartIndex();
      if (dbIndex + 1 < logStartIndex) {
        LOG.warn("{}: OM DB is at index {} but the raft log starts at index {}; entries {}..{} are "
                + "not available locally and must come from the leader.",
            groupLabel, dbIndex, logStartIndex, dbIndex + 1, logStartIndex - 1);
      }
      LOG.debug("{}: raft log end index {} covers OM DB index {}; nothing to align.",
          groupLabel, logEndIndex, dbIndex);
      return 0;
    }

    int deleted = 0;
    for (LogSegmentPath segment : segments) {
      Files.delete(segment.getPath());
      deleted++;
    }
    LOG.warn("{}: OM DB index {} is past the raft log end index {} (DB was replaced by a "
            + "snapshot but the raft log was not purged, see HDDS-15068). All {} raft log "
            + "segment file(s) only hold entries already included in the snapshot and were "
            + "deleted. Ratis will start with an empty log and continue from index {}.",
        groupLabel, dbIndex, logEndIndex, deleted, dbIndex + 1);
    return deleted;
  }

  /**
   * Fails fast with {@link OMRaftLogInconsistencyException} when the segment
   * files of the group do not form a contiguous index range (e.g.
   * {@code log_0-99} followed by {@code log_inprogress_150}), so the operator
   * gets an actionable message pointing at {@code ozone repair om raft-log}
   * instead of an opaque Ratis {@code IllegalStateException}.
   */
  public static void failOnInterSegmentGap(RaftStorage raftStorage, String groupLabel)
      throws IOException {
    final List<LogSegmentPath> segments = LogSegmentPath.getLogSegmentPaths(raftStorage);
    final File dir = raftStorage.getStorageDir().getCurrentDir();
    for (int i = 1; i < segments.size(); i++) {
      final LogSegmentPath prev = segments.get(i - 1);
      final LogSegmentPath curr = segments.get(i);
      final LogSegmentStartEnd prevRange = prev.getStartEnd();
      final LogSegmentStartEnd currRange = curr.getStartEnd();
      if (prevRange.isOpen()) {
        // An in-progress segment must be the last segment in the dir;
        // any segment following it indicates a corrupted layout.
        throw new OMRaftLogInconsistencyException(groupLabel
            + ": OM Ratis raft log has corrupted segment layout in " + dir
            + ": in-progress segment " + fileName(prev)
            + " is followed by " + fileName(curr)
            + ". Run 'ozone repair om raft-log inspect --raft-log-dir " + dir
            + "' to diagnose, then truncate with 'ozone repair om raft-log truncate"
            + " --raft-log-dir " + dir + " --index <last-good-index>'.");
      }
      if (currRange.getStartIndex() != prevRange.getEndIndex() + 1) {
        throw new OMRaftLogInconsistencyException(groupLabel
            + ": OM Ratis raft log has an inter-segment gap in " + dir
            + ": segment " + fileName(prev) + " ends at index " + prevRange.getEndIndex()
            + " but next segment " + fileName(curr) + " starts at index "
            + currRange.getStartIndex() + " (expected " + (prevRange.getEndIndex() + 1)
            + "). Run 'ozone repair om raft-log inspect --raft-log-dir " + dir
            + "' to diagnose, then 'ozone repair om raft-log truncate --raft-log-dir "
            + dir + " --index " + prevRange.getEndIndex() + "' to recover.");
      }
    }
  }

  /**
   * Last log index present on disk, computed the same way Ratis computes
   * {@code SegmentedRaftLogCache.getEndIndex()} at load time: closed segments
   * from their file names, the open segment by reading its entries. The open
   * segment is only read when it is the one thing that could still cover
   * {@code dbIndex}; otherwise the answer is already known from the names.
   */
  private static long computeLogEndIndex(RaftStorage raftStorage, List<LogSegmentPath> segments,
      SizeInBytes maxOpSize, long dbIndex, String groupLabel) throws IOException {
    long endIndex = RaftLog.INVALID_LOG_INDEX;
    LogSegmentPath openSegment = null;
    for (LogSegmentPath segment : segments) {
      final LogSegmentStartEnd range = segment.getStartEnd();
      if (range.isOpen()) {
        openSegment = segment;
      } else {
        endIndex = Math.max(endIndex, range.getEndIndex());
      }
    }
    if (openSegment == null || dbIndex <= endIndex) {
      return endIndex;
    }
    final LogSegmentStartEnd range = openSegment.getStartEnd();
    if (dbIndex < range.getStartIndex()) {
      // The open segment starts past the DB index, so the DB index is covered
      // whatever the segment holds (an empty open segment counts as start-1).
      return Math.max(endIndex, range.getStartIndex() - 1);
    }
    final int entries;
    try {
      entries = LogSegment.readSegmentFile(openSegment.getPath().toFile(), range, maxOpSize,
          raftStorage.getLogCorruptionPolicy(), null, null);
    } catch (IllegalStateException | IOException e) {
      // readSegmentFile asserts index contiguity (IllegalStateException) and
      // fails on checksum/header/size errors (IOException): the file is corrupt.
      final File dir = raftStorage.getStorageDir().getCurrentDir();
      throw new OMRaftLogInconsistencyException(groupLabel
          + ": OM Ratis raft log segment " + fileName(openSegment) + " in " + dir
          + " is corrupt (" + String.valueOf(e.getMessage()).replaceAll("\\s+", " ").trim() + ")."
          + " Run 'ozone repair om raft-log inspect --raft-log-dir " + dir
          + "' to locate the gap and 'ozone repair om raft-log truncate --raft-log-dir "
          + dir + " --index <last-good-index>' to recover.", e);
    }
    return Math.max(endIndex, range.getStartIndex() + entries - 1);
  }

  private static String fileName(LogSegmentPath segment) {
    return segment.getPath().getFileName().toString();
  }
}
