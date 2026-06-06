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

package org.apache.hadoop.ozone.repair.om.raftlog;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericCli;
import picocli.CommandLine;

/**
 * Subgroup that holds raft-log specific repair subcommands
 * ({@code inspect}, {@code truncate}). Invoked as
 * {@code ozone repair om raft-log <subcommand>}.
 */
@CommandLine.Command(name = "raft-log",
    description = "Inspect or truncate the OM Ratis segmented raft log on disk.",
    subcommands = {RaftLogInspect.class, RaftLogTruncate.class})
public class RaftLogRepair implements Callable<Void> {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--raft-log-dir"},
      required = true,
      description = "Path to the OM Ratis storage dir (the dir configured by "
          + "ozone.om.ratis.storage.dir, containing the raft group "
          + "subdirectories with 'current/log_*' segment files).")
  private String raftLogDir;

  public String getRaftLogDir() {
    return raftLogDir;
  }

  @Override
  public Void call() {
    GenericCli.missingSubcommand(spec);
    return null;
  }
}
