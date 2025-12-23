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

package org.apache.hadoop.ozone.admin.ratelimiter;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Subcommand for admin operations related to Rate Limiter.
 */
@CommandLine.Command(
        name = "ratelimiter",
        description = "Rate limiter specific admin operations",
        mixinStandardHelpOptions = true,
        versionProvider = HddsVersionProvider.class,
        subcommands = {
            RateLimiterCreateSubCommand.class,
            RateLimiterDeleteSubCommand.class,
            RateLimiterListSubCommand.class
        })
@MetaInfServices(AdminSubcommand.class)
public class RateLimiterCommands implements AdminSubcommand {
  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  public OzoneAdmin getParent() {
    return parent;
  }

  public RpcClient newRpcClient(OzoneConfiguration conf, String omServiceId) throws IOException {
    return new RpcClient(conf, omServiceId);
  }
}
