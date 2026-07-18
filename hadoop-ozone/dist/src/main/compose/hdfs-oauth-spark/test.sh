#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

#suite:oauth-hdfs-spark

# Self-contained bash suite — no Ozone dist, no robot. Storage is
# plain HDFS; the checks mirror ozone-oauth-spark/test.sh.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$COMPOSE_DIR" || exit 1

FAIL=0
fail() { echo "FAIL: $*"; FAIL=1; }
pass() { echo "PASS: $*"; }

hdfs_as_nn() {
  docker-compose exec -T -e OZONE_AGENT_LOG_LEVEL=OFF namenode "$@"
}

if [[ "${1:-}" != "--no-recreate" ]]; then
  docker-compose down -v 2>/dev/null
  docker-compose up -d
fi

echo "=== Waiting for Keycloak realm ==="
for i in $(seq 1 30); do
  if hdfs_as_nn curl -sf http://keycloak:8080/realms/EXAMPLE.COM > /dev/null 2>&1; then
    echo "Keycloak ready"; break
  fi
  sleep 5
done

echo "=== Waiting for HDFS ==="
hdfs_ready=false
for i in $(seq 1 60); do
  live=$(hdfs_as_nn hdfs dfsadmin -report 2>/dev/null | grep -c "^Name:")
  if [[ "${live:-0}" -ge 1 ]]; then
    hdfs_as_nn hdfs dfsadmin -safemode wait > /dev/null 2>&1
    hdfs_ready=true; echo "HDFS ready"; break
  fi
  sleep 5
done
$hdfs_ready || { fail "HDFS did not come up"; docker-compose logs --tail 30 namenode datanode; exit 1; }

echo "=== Preparing /spark-test workspace on HDFS ==="
hdfs_as_nn bash -c "
  hdfs dfs -mkdir -p /spark-test /user/spark &&
  hdfs dfs -chown -R spark /spark-test /user/spark
" || fail "could not prepare HDFS dirs"

echo "=== Waiting for spark-master port 7077 ==="
for i in $(seq 1 60); do
  if hdfs_as_nn bash -c "exec 3<>/dev/tcp/spark-master/7077" > /dev/null 2>&1; then
    echo "spark-master ready"; break
  fi
  sleep 5
done

echo "=== Waiting for spark-worker to register ==="
for i in $(seq 1 30); do
  if docker-compose logs spark-master 2>&1 | grep -c "Registering worker" > /dev/null; then
    echo "spark-worker registered"; break
  fi
  sleep 5
done

echo "=== SparkPi (driver + executors over OAuth) ==="
SPARK_EXAMPLES_JAR=$(docker-compose exec -T spark-master bash -c "ls /opt/spark/examples/jars/spark-examples_*.jar | head -1" | tr -d '\r[:space:]')
docker-compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    "${SPARK_EXAMPLES_JAR}" 4 > /tmp/spark-pi.out 2>&1
if grep -qE "Pi is roughly" /tmp/spark-pi.out; then
  pass "SparkPi completed"
else
  fail "SparkPi did not produce a result"; tail -10 /tmp/spark-pi.out
fi

echo "=== spark-sql DataFrame round-trip on hdfs:// ==="
WORKSPACE="hdfs://namenode:9000/spark-test"
docker-compose exec -T spark-master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 -e "
  DROP TABLE IF EXISTS spark_oauth;
  CREATE TABLE spark_oauth (n INT, label STRING) USING parquet
    LOCATION '${WORKSPACE}/spark_oauth';
  INSERT INTO spark_oauth VALUES (1, 'one'), (2, 'two'), (3, 'three');
  SELECT COUNT(*) FROM spark_oauth;
" > /tmp/spark-sql.out 2>&1
if grep -qE "^3$" /tmp/spark-sql.out; then
  pass "spark-sql round-trip returned 3"
else
  fail "spark-sql round-trip did not return 3"; tail -10 /tmp/spark-sql.out
fi

echo "=== spark-shell REPL round-trip on hdfs:// ==="
SHELL_WORKSPACE="${WORKSPACE}/shell_oauth"
cat <<EOF | docker-compose exec -T spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077 > /tmp/spark-shell.out 2>&1
val data = Seq((1,"a"), (2,"b"), (3,"c"), (4,"d"))
val df = spark.createDataFrame(data).toDF("n","label")
df.write.mode("overwrite").parquet("${SHELL_WORKSPACE}")
val cnt = spark.read.parquet("${SHELL_WORKSPACE}").count()
println(s"SHELL_RESULT_COUNT=\$cnt")
:quit
EOF
if grep -qE "SHELL_RESULT_COUNT=4" /tmp/spark-shell.out; then
  pass "spark-shell REPL round-trip returned 4"
else
  fail "spark-shell REPL did not print SHELL_RESULT_COUNT=4"
  grep -E "Exception|error" /tmp/spark-shell.out | head -5
fi

echo "=== HDFS-side state: parquet files owned by 'spark' ==="
owner=$(hdfs_as_nn hdfs dfs -stat '%u' /spark-test/spark_oauth 2>/dev/null | tr -d '[:space:]')
if [[ "$owner" == "spark" ]]; then
  pass "workspace dir owned by 'spark' (OAuth identity mapped)"
else
  fail "unexpected owner '$owner' for /spark-test/spark_oauth"
fi

echo "=== Teardown ==="
hdfs_as_nn hdfs dfs -rm -r -f -skipTrash /spark-test > /dev/null 2>&1
if hdfs_as_nn hdfs dfs -test -d /spark-test 2>/dev/null; then
  fail "post-drop: /spark-test still exists"
else
  pass "post-drop: spark workspace cleaned up"
fi

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit "$FAIL"
