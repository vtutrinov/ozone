<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
-->

# ozone-oauth-hive

Secure Ozone (OAuth-only, no KDC) with Apache Hive Metastore (HMS)
backed by Postgres.

This compose suite extends `ozone-oauth/` by adding:

- **postgres** — backing DB for HMS schema
- **hms** — Apache Hive Metastore (Hive 4.0.1), `hive --service
  metastore`. Loads `security-auth-agent` via `SERVICE_OPTS` so its
  Kerberos keytab login is intercepted; HMS authenticates to OM
  via OAuth (AUTH_LOGIN=hms).
- **hiveserver2** — Apache HiveServer2 (same image, `SERVICE_NAME=
  hiveserver2`). Talks to HMS over thrift (HMS thrift SASL is off
  in this suite — see hive-site.xml note) and to OM via OAuth.

## What this commit covers

End-to-end DDL through HS2 → HMS lands metadata in Postgres **and**
materialises the warehouse paths on Ozone (`ofs://om/volume1/bucket1
/warehouse`). Specifically the smoketest verifies:

- HMS thrift port reachable
- HiveServer2 thrift JDBC port reachable
- Agent banner present in HMS logs (OAuth login succeeded)
- `CREATE DATABASE LOCATION 'ofs://…'` materialises the DB dir on
  Ozone (owner = `hms`, the OAuth identity)
- `CREATE TABLE` materialises the table dir
- `DROP DATABASE CASCADE` removes everything cleanly

## Run

```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone-oauth-hive
OZONE_REPLICATION_FACTOR=3 ./test.sh
```

`test.sh` will fetch the postgres JDBC driver to `./jars/` on first
run (gitignored). No KDC needed.

## Followups (separate commits)

- **`INSERT` round-trip through HS2 local MR** — currently hangs
  in `KeyOutputStream → RatisBlockOutputStream` with the DN-side
  gRPC channel reporting `UNAVAILABLE: Network closed for unknown
  reason` after 18 retries. Not an auth issue (agent token-refresh
  is clean), but a streamed-write path issue in the Ozone client
  used by HS2's local MR. Tracked as a separate fix.
- **Tez execution engine** — Hive-on-Tez SQL workloads. The image
  already ships `/opt/tez/*` on the classpath; needs local-mode
  config in hive-site.xml plus tez-site.xml.
- **Hive thrift SASL interception** — HMS↔HS2 currently runs
  unauthenticated thrift because the agent's SASL hooks target
  Hadoop IPC's `SaslRpcServer`, not Hive's `HadoopThriftAuthBridge
  / TSaslServerTransport`. Adding interceptors for those classes
  is its own commit.
- **Delegation-token renewal probe** — long-running query that
  outlives the access-token TTL, asserts the proactive refresh
  keeps it alive (the `auth-token-renewal` change in this commit
  is the prerequisite).
- **HMS impersonation (`doAs`)** — verify ACL is checked against
  the client identity, not HMS's service identity.
