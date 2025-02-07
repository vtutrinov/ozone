/**
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

package org.apache.hadoop.ozone.shell.fsck;

import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;

/**
 * Class representing the level of details with which a report for an Ozone key will be printed.
 * There are four available levels as of now:
 * <ul>
 *   <li>{@link #KEY} - on this level {@link OzoneFsckWriter} writes only basic information about a key.
 *   <li>{@link #CONTAINER} - on this level
 *   {@link OzoneFsckWriter} adds additional information about containers where the key is located.
 *   <li>{@link #BLOCK} - on this level {@link OzoneFsckWriter} writes blocks for each where key data is stored.
 *   <li>{@link #CHUNK} - on this level {@link OzoneFsckWriter} prints the list of chunk per block.
 * </ul>
 */
public enum OzoneFsckVerbosityLevel {
  KEY(0),
  CONTAINER(1),
  BLOCK(2),
  CHUNK(3);

  private final int level;

  OzoneFsckVerbosityLevel(int level) {
    this.level = level;
  }

  /** Checks if current instance of {@link OzoneFsckVerbosityLevel} is right for a provided level. */
  boolean applicable(OzoneFsckVerbosityLevel expectedLevel) {
    return level >= expectedLevel.level;
  }
}
