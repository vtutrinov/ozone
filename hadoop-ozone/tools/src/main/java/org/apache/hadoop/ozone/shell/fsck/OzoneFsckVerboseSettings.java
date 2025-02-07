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

package org.apache.hadoop.ozone.shell.fsck;

import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.BLOCK;
import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.CHUNK;
import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.CONTAINER;

/**
 * It is a configuration holder class used to manage verbose settings for the Ozone File System Check (Fsck) operations.
 * It is used to determine the level of verbosity during the scan operations performed by the {@link OzoneFsckHandler}.
 */
public final class OzoneFsckVerboseSettings {
  private final boolean printHealthyKeys;

  private final OzoneFsckVerbosityLevel level;

  private OzoneFsckVerboseSettings(boolean printHealthyKeys, OzoneFsckVerbosityLevel level) {
    this.printHealthyKeys = printHealthyKeys;
    this.level = level;
  }

  /**
   * Indicates whether healthy keys should be printed during the Ozone File System Check (fscheck) operation.
   *
   * @return {@code true} if healthy keys should be printed; {@code false} otherwise.
   */
  public boolean printHealthyKeys() {
    return printHealthyKeys;
  }

  /**
   * Indicates whether containers should be printed during the Ozone File System Check (fscheck) operation.
   *
   * @return {@code true} if containers should be printed based on the verbosity level; {@code false} otherwise.
   */
  public boolean printContainers() {
    return level.applicable(CONTAINER);
  }

  /**
   * Indicates whether blocks should be printed during the Ozone File System Check (fscheck) operation.
   *
   * @return {@code true} if blocks should be printed based on the verbosity level; {@code false} otherwise.
   */
  public boolean printBlocks() {
    return level.applicable(BLOCK);
  }

  /**
   * Indicates whether chunks should be printed during the Ozone File System Check (fscheck) operation.
   *
   * @return {@code true} if chunks should be printed based on the verbosity level; {@code false} otherwise.
   */
  public boolean printChunks() {
    return level.applicable(CHUNK);
  }

  /**
   * Creates a new instance of the {@link SettingsBuilder} for configuring {@link OzoneFsckVerboseSettings}.
   *
   * @return a new instance of {@link SettingsBuilder}.
   */
  public static SettingsBuilder builder() {
    return new SettingsBuilder();
  }

  /**
   * A builder class used to configure and create instances of {@link OzoneFsckVerboseSettings}.
   * It allows setting the verbosity level and options
   * to control the output of the Ozone File System Check (fscheck) operation.
   */
  public static class SettingsBuilder {
    private boolean printKeys;

    private OzoneFsckVerbosityLevel verbosityLevel;

    /**
     * Sets whether healthy keys should be printed during the Ozone File System Check (fscheck) operation.
     *
     * @param printHealthyKeys a boolean indicating whether healthy keys should be printed.
     */
    public SettingsBuilder printHealthyKeys(boolean printHealthyKeys) {
      this.printKeys = printHealthyKeys;

      return this;
    }

    /**
     * Sets the verbosity level for the Ozone File System Check (fscheck) operation.
     *
     * @param level the desired verbosity level.
     */
    public SettingsBuilder level(OzoneFsckVerbosityLevel level) {
      this.verbosityLevel = level;

      return this;
    }

    /**
     * Builds and returns an instance of {@link OzoneFsckVerboseSettings}
     * configured with the current settings in the {@link SettingsBuilder}.
     */
    public OzoneFsckVerboseSettings build() {
      return new OzoneFsckVerboseSettings(printKeys, verbosityLevel);
    }
  }
}
