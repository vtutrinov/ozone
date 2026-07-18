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

# hdfs-oauth-flink: Flink Standalone over secure plain-HDFS + OAuth

Mirror of `ozone-oauth-flink` with the storage layer swapped from
Ozone to **stock Apache Hadoop HDFS** (no Ozone components, no KDC,
no keytabs). The Flink image ships no Hadoop; where the Ozone
variant fed it the Ozone dist lib dir, here a one-shot `hadooplibs`
compose service flattens the Hadoop client jars out of the stock
`apache/hadoop` image into a shared volume, consumed by JM/TM via
`HADOOP_CLASSPATH=/opt/hadooplibs/*`. The agent loads into every
Flink JVM (JM, TM, and the `flink run` CLI) via
`env.java.opts.all` in flink-conf.yaml.

## What the smoketest proves

| Check | Proof |
|-------|-------|
| Agent on JM/TM | `Installed Hadoop security auth agent` in both daemon logs |
| Batch WordCount hdfs→hdfs | `Program execution finished` + correct counts (`flink=3`) in the output files |
| Identity mapping | output files owned by `flink` (the `AUTH_LOGIN` identity) |

## Run

```bash
mvn -pl hadoop-ozone/security-auth-agent package -DskipTests   # once
cd hadoop-ozone/dist/src/main/compose/hdfs-oauth-flink
./test.sh                 # clean bring-up + all checks
./test.sh --no-recreate   # re-run checks against a running cluster
```

Version note: Flink 1.20.0 (LTS) — same pin as ozone-oauth-flink;
Flink 2.0 had unrelated startup issues on the smoketest machine.
