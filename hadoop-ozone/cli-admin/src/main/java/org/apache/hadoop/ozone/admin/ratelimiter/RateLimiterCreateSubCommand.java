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
package org.apache.hadoop.ozone.admin.ratelimiter;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Rate Limiter Create Subcommand.
 */
@CommandLine.Command(
        name = "create",
        description = "Create a new rate limiter.",
        mixinStandardHelpOptions = true,
        versionProvider = HddsVersionProvider.class)
public class RateLimiterCreateSubCommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private RateLimiterCommands parent;

  @CommandLine.Option(
          names = {"--volume"},
          description = "Volume name",
          required = true
  )
  private String volumeName;

  @CommandLine.Option(
          names = {"--bucket"},
          description = "Bucket name",
          required = true
  )
  private String bucketName;

  @CommandLine.Option(
          names = {"--rps"},
          description = "RPS",
          required = true
  )
  private int rps;

  @CommandLine.Option(
          names = {"--type"},
          description = "Rate limiter type",
          required = true
  )
  private String type;

  @CommandLine.Option(names = {"-id", "--service-id"},
          description = "OM Service ID",
          required = true)
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    try {
      RpcClient client = parent.newRpcClient(parent.getParent().getOzoneConf(), omServiceId);
      RateLimiterType rateLimiterType;
      try {
        rateLimiterType = RateLimiterType.valueOf(type.toUpperCase());
      } catch (IllegalArgumentException e) {
        System.err.println("Invalid rate limiter type. Use 'read' or 'write'.");
        return null;
      }
      if (rps <= 0) {
        System.err.println("RPS must be positive.");
        return null;
      }

      client.createRateLimiter(volumeName, bucketName, rps, rateLimiterType);
    } catch (Exception e) {
      System.err.println("Failed to create a rate limiter.");
    }
    return null;
  }

  @VisibleForTesting
  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  @VisibleForTesting
  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  @VisibleForTesting
  public void setRps(int rps) {
    this.rps = rps;
  }

  @VisibleForTesting
  public void setType(String type) {
    this.type = type;
  }
}
