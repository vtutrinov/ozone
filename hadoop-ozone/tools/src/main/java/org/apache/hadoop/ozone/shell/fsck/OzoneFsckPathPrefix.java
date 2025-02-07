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

import jakarta.annotation.Nullable;

/**
 * Represents a prefix path in the Ozone File System Check (fscheck) operation.
 * This class encapsulates the volume, bucket, and key that are used in fscheck operation in Ozone.
 */
public final class OzoneFsckPathPrefix {
  private final @Nullable String volume;

  private final @Nullable String bucket;

  private final @Nullable String key;

  OzoneFsckPathPrefix(@Nullable String volume, @Nullable String bucket, @Nullable String key) {
    this.volume = volume;
    this.bucket = bucket;
    this.key = key;
  }

  @Nullable String volume() {
    return volume;
  }

  @Nullable String bucket() {
    return bucket;
  }

  @Nullable String key() {
    return key;
  }
}
