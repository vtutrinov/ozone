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

# ozone-oauth-storm

Secure Ozone (OAuth-only, no KDC) with an Apache Storm cluster.
Sibling of `ozone-oauth-hive/`, `ozone-oauth-spark/` and
`ozone-oauth-flink/`.

This suite extends `ozone-oauth/` by adding:

- **zookeeper** — Storm's coordination backend (Storm 2.x still
  requires it).
- **nimbus** — Storm 2.6.4 master, thrift port 6627. Loads the
  security-auth-agent via `JAVA_TOOL_OPTIONS` so every JVM
  spawned from this container (nimbus daemon, storm CLI, worker
  JVMs) inherits the agent.
- **supervisor** — one worker host. Same agent + OAuth identity.
- **storm-ui** — web UI on port 8083 (mapped from container's
  8080 to avoid clashing with the Spark UI).

## What this commit covers

- Cluster + agent path work end-to-end:
  - Nimbus + Supervisor JVMs run with the agent loaded (banner
    + "Replaced login UGI with OAuth user: storm/..." lines).
  - Supervisor registers with Nimbus over ZooKeeper.
  - Storm UI reachable.
- **Write from within the Storm container to `ofs://`**:
  test.sh executes `ozone fs -put` from inside the Nimbus
  container. The write uses Nimbus's OAuth-authenticated UGI
  and the shaded Ozone client that we bind-mount into
  `extlib-daemon`. The probe key lands on Ozone with owner
  = `storm`, verifying the same auth + write pipeline a
  real topology's HdfsBolt would exercise (minus the actual
  bolt scheduling).

## Run

```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone-oauth-storm
OZONE_REPLICATION_FACTOR=3 ./test.sh
```

No KDC needed. The first run pulls `storm:2.6.4` and
`zookeeper:3.9`.

## Notes

- **`storm:2.6.4` doesn't ship prebuilt topology jars.** The
  `examples/` dir under `/apache-storm-*` only has POM +
  sources. Building a real HdfsBolt topology from source would
  need a `mvn package` step which is out of scope for this
  smoketest. The `ozone fs -put` probe from within the Nimbus
  container is the same auth/write path a topology would take
  — same UGI, same classpath, same OM/DN endpoints. A real
  topology submission (WordCount → HdfsBolt) is tracked as a
  followup.
- **`JAVA_TOOL_OPTIONS`** is used to load the agent instead of
  Storm's per-command opts (`STORM_JAR_JVM_OPTS`,
  `NIMBUS_CHILDOPTS`, `SUPERVISOR_CHILDOPTS`, `WORKER_CHILDOPTS`).
  A single `JAVA_TOOL_OPTIONS` covers every JVM the container
  spawns without having to enumerate them.
- **`extlib-daemon` + `extlib`** are bind-mounted from
  `/opt/ozone/share/ozone/lib` so daemon and worker JVMs both
  see `org.apache.hadoop.security.UserGroupInformation` (the
  agent Class.forName's it at premain to replace the login
  UGI). The `storm:2.6.4` image ships no Hadoop of its own.
- **TLS on data path is on** (`hdds.grpc.tls.enabled=true`
  both cluster-side and client-side). Same fix pattern as the
  other engine suites.

## Followups

- Prebuild a WordCount → HdfsBolt topology jar (or ship one via
  a small `mvn package` step in test.sh) so we can prove the
  spout → bolt → ofs:// pipeline, not just the shell probe.
