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
- `INSERT` writes a data file (`{table_dir}/000000_0`) via Tez —
  the Hive 4 default execution engine, running in local mode
  (`tez.local.mode=true`, no YARN needed). Proves the end-to-end
  write path: HS2 → Tez AM/task in-JVM → OzoneClient → OM (OAuth)
  → DN block write → ofs commit
- A `COUNT(*)` follow-up query exercises a multi-vertex Tez DAG
  and asserts the row count matches the inserts
- **DT-renewal probe**: a 100-second sleep query (via
  `reflect('java.lang.Thread','sleep',…)`) straddles the realm's
  90-second access-token TTL. The agent's proactive refresher must
  swap in a fresh token mid-flight; the smoketest asserts at least
  one `"Proactively refreshed OAuth token"` line in HS2's docker log
- `DROP DATABASE CASCADE` removes everything cleanly

## Run

```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone-oauth-hive
OZONE_REPLICATION_FACTOR=3 ./test.sh
```

`test.sh` will fetch the postgres JDBC driver to `./jars/` on first
run (gitignored). No KDC needed.

## Caveats

- **`hdds.grpc.tls.enabled=false`** in this suite's docker-config.
  Default secure-cluster setting puts TLS on the client→DN Ratis
  gRPC channel, which requires the client to trust SCM's root CA.
  The apache/hive image isn't an ozone-runner — it has no
  SCM-issued certs and no truststore — so the TLS handshake closes
  silently and any `INSERT` hangs at 67% map with
  `UNAVAILABLE: Network closed for unknown reason`. We drop TLS
  here; block-token auth at the application layer is still on.
  Proper fix: a sidecar that fetches SCM's root CA into a
  bind-mounted truststore the Hive containers can read. Tracked
  as a followup.

## Followups (separate commits)

- **HMS impersonation (`doAs`)** — verify ACL is checked against
  the client identity, not HMS's service identity.
- **Re-enable `hdds.grpc.tls.enabled`** for the Hive containers
  via a CA-distribution sidecar — see Caveats above.
