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

# hdfs-oauth-hive: Hive 4 on Tez over secure plain-HDFS + OAuth

Mirror of `ozone-oauth-hive` with the storage layer swapped from
Ozone to **stock Apache Hadoop HDFS** (no Ozone components, no KDC,
no keytabs). Warehouse lives on `hdfs://namenode:9000/warehouse`;
the security-auth-agent covers the NN/DN RPC control plane, the HMS
thrift SASL handshake (via the JCA `OAuthSaslProvider`), and every
Hive JVM's `loginUserFromKeytab`.

## What the smoketest proves

| Check | Proof |
|-------|-------|
| DDL through HMS to HDFS | `CREATE DATABASE/TABLE` materialises dirs under `/warehouse` |
| INSERT via Tez DAG | `COUNT(*) = 3` + `tez.TezTask` hits in hive.log |
| doAs / impersonation | INSERT-written data files owned by `testuser` (beeline `-n testuser`, `hadoop.proxyuser.hms.*` honored by the NameNode) |
| Proactive token refresh | 100s sleep query straddles the 90s `accessTokenLifespan`; `Proactively refreshed OAuth token` in HS2 log |
| DROP cleanup | warehouse subdir gone post-`DROP DATABASE CASCADE` |

## Ozone-suite differences worth knowing

- **POSIX permissions actually enforce.** On Ozone this suite's
  ancestor passed with `ozone.administrators=*` (every authenticated
  user was an admin). Plain HDFS enforces the mode bits: HMS creates
  db/table dirs as `hms` (755), so `test.sh` chmods the fresh db dir
  to 777 before the doAs INSERT. The proof survives: the data files
  the INSERT writes carry `testuser` as owner.
- **No filesystem jar juggling.** `hdfs://` is native to the Hadoop
  libs the `apache/hive` image ships — no `HIVE_AUX_JARS_PATH`, no
  shaded ozone-filesystem jar, no ozone-site.xml.
- Tez runs in **local mode** inside HS2 (same as the ozone suite):
  Hive 4 removed the `mr` engine, and this keeps the suite free of a
  YARN dependency (YARN-over-OAuth is proven by `../hdfs-oauth`).

## Run

```bash
mvn -pl hadoop-ozone/security-auth-agent package -DskipTests   # once
cd hadoop-ozone/dist/src/main/compose/hdfs-oauth-hive
./test.sh                 # clean bring-up + all checks
./test.sh --no-recreate   # re-run checks against a running cluster
```
