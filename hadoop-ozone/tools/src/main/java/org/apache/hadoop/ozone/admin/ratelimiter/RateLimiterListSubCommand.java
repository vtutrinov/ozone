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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListRateLimiterResponse;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Rate Limiter List Subcommand.
 */
@CommandLine.Command(
        name = "list",
        description = "List all rate limiters.",
        mixinStandardHelpOptions = true,
        versionProvider = HddsVersionProvider.class)
public class RateLimiterListSubCommand implements Callable<Void> {
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

  @CommandLine.Option(names = {"-id", "--service-id"},
          description = "OM Service ID",
          required = true)
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    RpcClient client = parent.newRpcClient(parent.getParent().getOzoneConf(), omServiceId);
    ListRateLimiterResponse response = client.listRateLimiter(volumeName, bucketName);
    System.out.println(response);
    return null;
  }
}
