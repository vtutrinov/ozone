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

# ozone-oauth-spark

Secure Ozone (OAuth-only, no KDC) with an Apache Spark Standalone
cluster. Parallels `ozone-oauth-hive/` for the Spark engine.

This suite extends `ozone-oauth/` by adding:

- **spark-master** — Spark 3.5.x standalone master. Loads
  `security-auth-agent` via `SPARK_DAEMON_JAVA_OPTS` and runs as
  user `spark` over OAuth (`AUTH_LOGIN=spark`). RPC port 7077, web
  UI on 8081.
- **spark-worker** — joins `spark-master`. Same agent + OAuth
  identity. Executors that it spawns inherit the
  `spark.executor.extraJavaOptions` from `spark-defaults.conf` so
  they're agent-instrumented too.

## What this commit covers

End-to-end Spark workloads against `ofs://`:

- `nc -z spark-master 7077` / `8081` reachable
- **SparkPi**: `spark-submit` SparkPi to the standalone master.
  Exercises driver+executor JVMs going through the agent's OAuth
  flow; asserts the "Pi is roughly" line.
- **DataFrame round-trip**: `spark-sql` creates a parquet table
  at `ofs://om/volume1/bucket1/spark-test/spark_oauth/`, inserts
  three rows, runs `SELECT COUNT(*) = 3`. Proves the end-to-end
  write path: Spark driver -> OzoneClient -> OM (OAuth) -> DN
  block write (TLS) -> ofs commit.
- **spark-shell REPL**: test.sh pipes a Scala snippet into
  `spark-shell` and asserts the printed `SHELL_RESULT_COUNT=4`.
  Verifies the REPL path through the agent is equivalent to
  batch `spark-submit`.
- Robot assertion that the resulting Parquet part files exist on
  Ozone and are owned by `spark` (the OAuth identity), not by
  whichever OS user the executor JVM happens to be.

## Run

```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone-oauth-spark
OZONE_REPLICATION_FACTOR=3 ./test.sh
```

No KDC needed. The first run pulls `apache/spark:3.5.3`.

## Notes

- **TLS on data path is on** (`hdds.grpc.tls.enabled=true` both
  in `docker-config` for OM/SCM/DN and in `ozone-site.xml` for
  the Spark containers). Same fix as `ozone-oauth-hive/` —
  external clients need the property set explicitly so Netty/gRPC
  doesn't default to plaintext against a TLS-listening DN port.
- **Standalone mode**, not YARN. Lets us run Spark with a single
  master + worker without bringing up RM/NM/JHS. Spark on YARN
  with OAuth is a separate (larger) integration.
- **spark-shell** REPL probe is included too: test.sh pipes a
  small Scala snippet (`Seq.toDF.write.parquet → read.parquet
  → count`) into `spark-shell` and asserts the printed
  `SHELL_RESULT_COUNT=4`. The REPL loads the same agent via
  `spark.driver.extraJavaOptions`, so it exercises the same
  OAuth-authenticated driver path as `spark-submit` does.
