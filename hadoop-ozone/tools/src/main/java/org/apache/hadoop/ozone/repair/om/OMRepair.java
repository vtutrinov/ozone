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

package org.apache.hadoop.ozone.repair.om;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.hadoop.ozone.repair.om.raftlog.RaftLogRepair;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Ozone Repair CLI subgroup for OM-specific repair commands that operate
 * directly on the OM raft-log files on disk. Sits as a peer of
 * {@link org.apache.hadoop.ozone.repair.RDBRepair} under {@link OzoneRepair}
 * because raft-log operations do not need the {@code --db} flag.
 */
@CommandLine.Command(name = "om",
    description = "Operational tools to repair OM raft-log and metadata.",
    subcommands = {RaftLogRepair.class})
@MetaInfServices(SubcommandWithParent.class)
public class OMRepair implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public Void call() {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneRepair.class;
  }
}
