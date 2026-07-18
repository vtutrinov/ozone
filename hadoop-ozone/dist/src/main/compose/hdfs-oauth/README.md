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

# hdfs-oauth: secure plain-HDFS/YARN over OAuth — no Ozone, no KDC

All `ozone-oauth*` suites prove the security-auth-agent against Ozone
storage. This suite removes Ozone entirely and runs **stock Apache
Hadoop** (`ghcr.io/apache/hadoop:3.4.2-lean`): NameNode, DataNode,
ResourceManager, NodeManager — with `hadoop.security.authentication=kerberos`,
service principals configured, and **no KDC and no keytab files
anywhere**. The agent intercepts `loginUserFromKeytab()` and the SASL
GSSAPI exchanges, substituting OAuth tokens from Keycloak. Since the
agent only depends on `hadoop-common` (provided scope), this is the
storage-agnostic proof: nothing Ozone-specific is needed for it to
secure a vanilla Hadoop cluster.

## What the smoketest proves

| # | Check | Proof |
|---|-------|-------|
| 1 | Agent loaded on NN/DN/RM/NM | `Installed Hadoop security auth agent` in each service log |
| 2 | RPC callers authenticated via OAuth | `Auth successful for nn/namenode@EXAMPLE.COM (auth:OAUTH)` in NN log |
| 3 | HDFS write + read | `hdfs dfs -copyFromLocal` / `-cat` round-trip, byte-identical |
| 4 | OAuth identity → HDFS user | file owner is `hadoop` after writing as `AUTH_LOGIN=hadoop` |
| 5 | Permission enforcement | `testuser` denied deleting `hadoop`'s file |
| 6 | MapReduce over YARN | `pi` completes; `wordcount` output on `hdfs://` has correct counts |

The data-transfer path (client ↔ DataNode) uses SASL DIGEST-MD5 keyed
by **block access tokens** (`dfs.block.access.token.enable=true`,
`dfs.data.transfer.protection=authentication`) — that path never
involves Kerberos, so it runs natively; only the RPC control plane
needed the agent. `ignore.secure.ports.for.testing=true` lets the
DataNode run SASL on non-privileged ports without HTTPS.

## Differences from the ozone-oauth suites

- **No Ozone dist tarball needed.** The only build artifact used is
  `security-auth-agent-*.jar` (mounted from the module's `target/`,
  see `AGENT_JAR` in `.env`). Build it with:
  `mvn -pl hadoop-ozone/security-auth-agent package -DskipTests`
- **Self-contained bash test** (`./test.sh`), no robot/testlib: the
  suite is runnable directly from `src/main/compose/hdfs-oauth`.
- Per-exec identity: the agent's `env` provider reads
  `AUTH_LOGIN`/`AUTH_PASSWORD` from the process env, so
  `docker-compose exec -e AUTH_LOGIN=testuser ...` switches OAuth
  identity per command — that is how the multi-user permission test
  works with a single container.

## Run

```bash
cd hadoop-ozone/dist/src/main/compose/hdfs-oauth
./test.sh                 # clean bring-up + all checks
./test.sh --no-recreate   # re-run checks against a running cluster
```
