/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import io.netty.handler.codec.CodecException;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType;

/**
 * Helper class that represents rate limiter configuration in OM metadata.
 */
public final class RateLimiterInfo {

  private static final Codec<RateLimiterInfo> CODEC = new DelegatedCodec<>(
          Proto2Codec.get(RateLimiter.getDefaultInstance()),
          RateLimiterInfo::getFromProtobuf,
          RateLimiterInfo::getProtobuf,
          DelegatedCodec.CopyType.SHALLOW);

  public static Codec<RateLimiterInfo> getCodec() {
    return CODEC;
  }

  private final String volumeName;
  private final String bucketName;
  private final int rps;
  private final RateLimiterType type;

  private RateLimiterInfo(Builder b) {
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.rps = b.rps;
    this.type = b.type;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public int getRps() {
    return rps;
  }

  public RateLimiterType getType() {
    return type;
  }

  public static RateLimiterInfo getFromProtobuf(
          RateLimiter proto) throws CodecException {
    return new Builder()
            .setVolumeName(proto.getVolumeName())
            .setBucketName(proto.getBucketName())
            .setRps(proto.getRps())
            .setType(proto.getType())
            .build();
  }

  public RateLimiter getProtobuf() {
    return RateLimiter.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setRps(rps)
            .setType(type)
            .build();
  }

  public RateLimiter toProtobuf() {
    return RateLimiter.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setRps(rps)
            .setType(type)
            .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for {@link RateLimiterInfo}.
   */
  public static final class Builder {
    private String volumeName;
    private String bucketName;
    private int rps;
    private RateLimiterType type;

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setRps(int rps) {
      this.rps = rps;
      return this;
    }

    public Builder setType(RateLimiterType type) {
      this.type = type;
      return this;
    }

    public RateLimiterInfo build() {
      if (volumeName == null || volumeName.isEmpty()) {
        throw new IllegalArgumentException("volumeName must not be empty");
      }
      if (bucketName == null || bucketName.isEmpty()) {
        throw new IllegalArgumentException("bucketName must not be empty");
      }
      if (rps <= 0) {
        throw new IllegalArgumentException("rps must be positive");
      }
      if (type == null) {
        throw new IllegalArgumentException("type must not be null");
      }
      return new RateLimiterInfo(this);
    }
  }

  @Override
  public String toString() {
    return "RateLimiterInfo{" +
            "volumeName='" + volumeName + '\'' +
            ", bucketName='" + bucketName + '\'' +
            ", rps=" + rps +
            ", type=" + type +
            '}';
  }
}
