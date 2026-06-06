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

package org.apache.hadoop.ozone.repair.om.raftlog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;

/**
 * Start / end index of a Ratis segment file, parsed from its name ({@code log_<start>-<end>} or
 * {@code log_inprogress_<start>}): the accessors of {@code LogSegmentStartEnd} are not public in Ratis 3.2.
 */
final class SegmentNames {
  private static final Pattern CLOSED = Pattern.compile("log_(\\d+)-(\\d+)");
  private static final Pattern OPEN = Pattern.compile("log_inprogress_(\\d+)");

  private SegmentNames() {
  }

  static boolean isOpen(LogSegmentPath seg) {
    return OPEN.matcher(name(seg)).matches();
  }

  static long start(LogSegmentPath seg) {
    Matcher closed = CLOSED.matcher(name(seg));
    if (closed.matches()) {
      return Long.parseLong(closed.group(1));
    }
    Matcher open = OPEN.matcher(name(seg));
    if (open.matches()) {
      return Long.parseLong(open.group(1));
    }
    throw new IllegalArgumentException("Not a raft log segment: " + seg.getPath());
  }

  /** @return the end index of a closed segment, -1 for an open one */
  static long end(LogSegmentPath seg) {
    Matcher closed = CLOSED.matcher(name(seg));
    return closed.matches() ? Long.parseLong(closed.group(2)) : -1L;
  }

  private static String name(LogSegmentPath seg) {
    return seg.getPath().getFileName().toString();
  }
}
