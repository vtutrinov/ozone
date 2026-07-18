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

# hdfs-oauth-spark: Spark Standalone over secure plain-HDFS + OAuth

Mirror of `ozone-oauth-spark` with the storage layer swapped from
Ozone to **stock Apache Hadoop HDFS** (no Ozone components, no KDC,
no keytabs). Spark 4.0.0 bundles the Hadoop 3.4 client, so unlike
the Ozone variant there is no filesystem-jar juggling at all — the
only injected artifact is the security-auth-agent jar, loaded into
the master/worker daemons (`SPARK_DAEMON_JAVA_OPTS`) and into every
driver/executor (`spark.driver/executor.extraJavaOptions`).

## What the smoketest proves

| Check | Proof |
|-------|-------|
| SparkPi | driver + executors run under the standalone master with OAuth-intercepted UGI |
| spark-sql batch | parquet table on `hdfs://namenode:9000/spark-test`, `INSERT` + `COUNT(*) = 3` |
| spark-shell REPL | 4-row parquet write/read round-trip, `SHELL_RESULT_COUNT=4` |
| Identity mapping | workspace dirs owned by `spark` (the `AUTH_LOGIN` identity) |

## Run

```bash
mvn -pl hadoop-ozone/security-auth-agent package -DskipTests   # once
cd hadoop-ozone/dist/src/main/compose/hdfs-oauth-spark
./test.sh                 # clean bring-up + all checks
./test.sh --no-recreate   # re-run checks against a running cluster
```
