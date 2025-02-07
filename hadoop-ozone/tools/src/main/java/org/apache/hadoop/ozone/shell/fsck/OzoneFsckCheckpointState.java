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

package org.apache.hadoop.ozone.shell.fsck;

/**
 * OzoneFsckCheckpointState is responsible for changing the state of a checkpoint.
 */
public class OzoneFsckCheckpointState {
  private String lastCheckedVolume;
  private String lastCheckedBucket;
  private String lastCheckedKey;

  public OzoneFsckCheckpointState() {
  }

  public OzoneFsckCheckpointState(String lastCheckedVolume, String lastCheckedBucket, String lastCheckedKey) {
    this.lastCheckedVolume = lastCheckedVolume;
    this.lastCheckedBucket = lastCheckedBucket;
    this.lastCheckedKey = lastCheckedKey;
  }

  public String getLastCheckedVolume() {
    return lastCheckedVolume;
  }

  public void setLastCheckedVolume(String lastCheckedVolume) {
    this.lastCheckedVolume = lastCheckedVolume;
  }

  public String getLastCheckedBucket() {
    return lastCheckedBucket;
  }

  public void setLastCheckedBucket(String lastCheckedBucket) {
    this.lastCheckedBucket = lastCheckedBucket;
  }

  public String getLastCheckedKey() {
    return lastCheckedKey;
  }

  public void setLastCheckedKey(String lastCheckedKey) {
    this.lastCheckedKey = lastCheckedKey;
  }
}
