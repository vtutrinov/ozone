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

# ozone-oauth-flink

Secure Ozone (OAuth-only, no KDC) with an Apache Flink session
cluster. Sibling of `ozone-oauth-hive/` and `ozone-oauth-spark/`.

This suite extends `ozone-oauth/` by adding:

- **jobmanager** — Flink 1.20.0 JobManager. REST/Web UI on port
  8081 (exposed as 8082 on the host to avoid clashing with
  Spark's default). Loads `security-auth-agent` via
  `env.java.opts.all` (in the bind-mounted `flink-conf.yaml`) and
  authenticates to Ozone as user `flink` over OAuth.
- **taskmanager** — one Task Manager with 2 slots, registers
  with `jobmanager` over the internal Pekko RPC. Same agent +
  OAuth identity. Executor JVMs inherit the agent through Flink's
  standard TM → task worker fork.

## What this commit covers

End-to-end Flink workload against `ofs://`:

- `nc -z jobmanager 8081` reachable inside the compose network.
- **Batch WordCount**: `flink run /opt/flink/examples/batch/
  WordCount.jar --input ofs://…/input.txt --output ofs://…/
  wc-output`. Reads a 3-line corpus we stage on Ozone, writes
  per-word counts back to a different ofs:// key. Exercises
  JobManager scheduling, TaskManager execution, and the Flink
  FileSystem adapter going through the OAuth-authenticated UGI.
- Robot assertions on the resulting `wc-output` dir:
  - at least one output part file exists;
  - the file owner on Ozone is `flink`;
  - `ozone 3` appears in the WC output (sanity-check that the
    job actually processed the data, not just emitted empties).
- `DROP` of the workspace + POST-DROP check that nothing lingers
  on ofs://.

## Run

```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone-oauth-flink
OZONE_REPLICATION_FACTOR=3 ./test.sh
```

No KDC needed. The first run pulls `apache/flink:1.20.0`.

## Notes

- **TLS on data path is on** (`hdds.grpc.tls.enabled=true` both
  cluster-side in `docker-config` and client-side in
  `ozone-site.xml`). Same fix as the Hive / Spark suites.
- **Flink Standalone session cluster**, not Flink-on-YARN /
  Flink-on-K8s. Lets us run the engine with a single
  JobManager + TaskManager pair, no resource-manager
  dependency.
- **DataSet API WordCount** is what the image ships. The
  DataSet API is deprecated in Flink 2.0; when we move to
  Flink 2.x we'll switch the probe to the DataStream / Table
  API equivalent.
- **`JOB_MANAGER_RPC_ADDRESS: jobmanager`** is set explicitly on
  the taskmanager service. Without it, `apache/flink`'s
  docker-entrypoint appends `jobmanager.rpc.address: $(hostname
  -f)` — which on the TM container becomes `taskmanager` and
  makes the TM try to reach itself for the JM RPC. The
  handshake fails silently in Pekko's remoting layer, and the
  cluster looks half-up until the TM's registration deadline
  expires.
- **`flink-conf.yaml`** is bind-mounted directly over
  `/opt/flink/conf/flink-conf.yaml` rather than passed via the
  `FLINK_PROPERTIES` env var. The YAML block scalar in
  `FLINK_PROPERTIES` was collapsing the whitespace inside our
  long `env.java.opts.all` line and mashing the `-javaagent`
  argument together with the `-Djava.security.krb5.*` sysprops,
  which broke the agent's arg parser.
- **`HADOOP_CLASSPATH`** points at
  `/opt/ozone/share/ozone/lib/*` — that's enough for Flink's
  `HadoopFsFactory` (in `flink-dist.jar`) to bridge `ofs://` to
  Ozone's `RootedOzoneFileSystem`. No bind-mounts into
  `/opt/flink/lib` are needed.
