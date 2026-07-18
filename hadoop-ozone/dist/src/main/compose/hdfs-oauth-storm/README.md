<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# hdfs-oauth-storm: Storm 2.6 over secure plain-HDFS + OAuth

Mirror of `ozone-oauth-storm` with the storage layer swapped from
Ozone to **stock Apache Hadoop HDFS** (no Ozone components, no KDC,
no keytabs). The Storm image ships no Hadoop; a one-shot
`hadooplibs` compose service flattens the Hadoop client jars from
the stock `apache/hadoop` image into a shared volume, mounted into
Storm's `extlib-daemon` (nimbus/supervisor/ui classpath) and
`extlib` (worker classpath). The agent loads into every JVM the
Storm containers spawn via `JAVA_TOOL_OPTIONS`.

## What the smoketest proves

| Check | Proof |
|-------|-------|
| Agent on Nimbus/Supervisor | `Installed Hadoop security auth agent` in both daemon logs |
| hdfs:// write from Nimbus JVM | `FsShell -put` (agent via JAVA_TOOL_OPTIONS, hdfs client from hadooplibs) — same auth+write pipeline an HdfsBolt uses |
| Identity mapping | probe file owned by `storm` (the `AUTH_LOGIN` identity), content round-trips |

Same honest caveat as the ozone variant: the probe exercises the
Storm container/JVM environment (classpath + agent + OAuth flow),
not an actual scheduled topology.

## Run

```bash
mvn -pl hadoop-ozone/security-auth-agent package -DskipTests   # once
cd hadoop-ozone/dist/src/main/compose/hdfs-oauth-storm
./test.sh                 # clean bring-up + all checks
./test.sh --no-recreate   # re-run checks against a running cluster
```
