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

#suite:oauth-spark

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# docker-compose reads .env automatically, bash doesn't.
if [[ -f "$COMPOSE_DIR/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$COMPOSE_DIR/.env"
  set +a
fi

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true
export SECURITY_OAUTH_ENABLED=true

start_docker_env

echo "Waiting for Keycloak..."
for i in $(seq 1 30); do
  if docker-compose exec -T scm curl -sf http://keycloak:8080/realms/EXAMPLE.COM > /dev/null 2>&1; then
    echo "Keycloak ready"
    break
  fi
  sleep 5
done

echo "Pre-creating volume1/bucket1 for the Spark workspace..."
docker-compose exec -T scm bash -c "
  ozone sh volume info /volume1 >/dev/null 2>&1 || \
    ozone sh volume create /volume1 --user spark --space-quota 100TB --namespace-quota 100
  ozone sh bucket info /volume1/bucket1 >/dev/null 2>&1 || \
    ozone sh bucket create /volume1/bucket1 --space-quota 1TB --layout fso
" 2>&1 | tail -3

echo "Waiting for spark-master REST port 7077..."
for i in $(seq 1 60); do
  if docker-compose exec -T scm nc -z spark-master 7077 > /dev/null 2>&1; then
    echo "spark-master is ready"
    break
  fi
  sleep 5
done

echo "Waiting for spark-worker to register..."
for i in $(seq 1 30); do
  if docker-compose logs spark-master 2>&1 | grep -q "Registering worker"; then
    echo "spark-worker registered"
    break
  fi
  sleep 5
done

# ----- SparkPi probe -----
# Use file-based capture instead of $(...). spark-submit's output
# is large (lots of INFO lines) and can confuse bash's command
# substitution under -u -o pipefail. The example jar name carries
# the scala+spark version, so glob inside the container via bash.
SPARK_EXAMPLES_JAR=$(docker-compose exec -T spark-master bash -c "ls /opt/spark/examples/jars/spark-examples_*.jar | head -1" | tr -d '\r')
echo "Using example jar: ${SPARK_EXAMPLES_JAR}"
echo "Running SparkPi to exercise driver+executor + OAuth..."
docker-compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    "${SPARK_EXAMPLES_JAR}" 4 > /tmp/spark-pi.out 2>&1 || true
tail -5 /tmp/spark-pi.out
if ! grep -qE "Pi is roughly" /tmp/spark-pi.out; then
  echo "FAIL: SparkPi did not produce a 'Pi is roughly' line"
  exit 1
fi

# ----- DataFrame round-trip on ofs:// -----
WORKSPACE="ofs://om/volume1/bucket1/spark-test"
echo "Running DataFrame write/read round-trip to ${WORKSPACE}..."
docker-compose exec -T spark-master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 -e "
  CREATE TABLE spark_oauth (n INT, label STRING) USING parquet
    LOCATION '${WORKSPACE}/spark_oauth';
  INSERT INTO spark_oauth VALUES (1, 'one'), (2, 'two'), (3, 'three');
  SELECT COUNT(*) FROM spark_oauth;
" > /tmp/spark-sql.out 2>&1 || true
tail -10 /tmp/spark-sql.out
# spark-sql prints the COUNT result on its own line (no table
# borders, just the number) followed by "Time taken: ...".
if ! grep -qE "^3$" /tmp/spark-sql.out; then
  echo "FAIL: spark-sql DataFrame round-trip did not return 3"
  exit 1
fi

# ----- spark-shell REPL probe -----
# Pipes a small Scala snippet into spark-shell's stdin. The shell
# initialises a SparkContext under the standalone master, writes a
# 4-row parquet to ofs://, reads it back, and println's the count.
# spark-shell ALSO loads the agent via
# spark.driver.extraJavaOptions, so this is the REPL counterpart
# to the spark-submit batch probe.
SHELL_WORKSPACE="${WORKSPACE}/shell_oauth"
echo "Running spark-shell REPL round-trip to ${SHELL_WORKSPACE}..."
cat <<EOF | docker-compose exec -T spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077 > /tmp/spark-shell.out 2>&1 || true
val data = Seq((1,"a"), (2,"b"), (3,"c"), (4,"d"))
val df = spark.createDataFrame(data).toDF("n","label")
df.write.mode("overwrite").parquet("${SHELL_WORKSPACE}")
val cnt = spark.read.parquet("${SHELL_WORKSPACE}").count()
println(s"SHELL_RESULT_COUNT=\$cnt")
:quit
EOF
grep -E "SHELL_RESULT_COUNT|Exception" /tmp/spark-shell.out | head -3
if ! grep -qE "SHELL_RESULT_COUNT=4" /tmp/spark-shell.out; then
  echo "FAIL: spark-shell REPL did not print SHELL_RESULT_COUNT=4"
  exit 1
fi

# Robot tests run inside scm — they verify the Ozone-side state.
execute_robot_test scm -v WORKSPACE:"${WORKSPACE}" security/spark.robot

echo "Teardown: drop the spark-test workspace..."
docker-compose exec -T scm bash -c "
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -rm -r -f -skipTrash ${WORKSPACE} 2>&1 | tail -1
"
docker-compose exec -T scm bash -c "
  if ozone fs -test -d ${WORKSPACE} 2>/dev/null; then
    echo 'POST-DROP FAIL: ${WORKSPACE} still exists'
    exit 1
  else
    echo 'POST-DROP OK: spark workspace cleaned up'
  fi
"
