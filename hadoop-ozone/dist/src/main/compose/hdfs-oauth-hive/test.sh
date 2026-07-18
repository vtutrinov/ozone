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

#suite:oauth-hdfs-hive

# Self-contained bash suite — no Ozone dist, no robot. Storage is
# plain HDFS; the checks mirror ozone-oauth-hive/test.sh.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$COMPOSE_DIR" || exit 1

FAIL=0
fail() { echo "FAIL: $*"; FAIL=1; }
pass() { echo "PASS: $*"; }

# Fetch postgres JDBC driver if not already present — the
# apache/hive image doesn't ship it.
POSTGRES_JDBC_VERSION=${POSTGRES_JDBC_VERSION:-42.7.4}
PG_JAR="$COMPOSE_DIR/jars/postgresql.jar"
if [[ ! -f "$PG_JAR" ]]; then
  mkdir -p "$COMPOSE_DIR/jars"
  echo "Downloading postgres JDBC driver ${POSTGRES_JDBC_VERSION}..."
  curl -sfL -o "$PG_JAR" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
fi

if [[ "${1:-}" != "--no-recreate" ]]; then
  docker-compose down -v 2>/dev/null
  docker-compose up -d
fi

hdfs_as_nn() {
  docker-compose exec -T -e OZONE_AGENT_LOG_LEVEL=OFF namenode "$@"
}

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

echo "=== Preparing /warehouse and /user dirs on HDFS ==="
hdfs_as_nn bash -c "
  hdfs dfs -mkdir -p /warehouse /user/testuser /user/hms /tmp &&
  hdfs dfs -chmod 1777 /warehouse /tmp &&
  hdfs dfs -chown testuser /user/testuser &&
  hdfs dfs -chown hms /user/hms
" || fail "could not prepare HDFS dirs"

echo "=== Waiting for HMS thrift port 9083 ==="
for i in $(seq 1 60); do
  if hdfs_as_nn bash -c "exec 3<>/dev/tcp/hms/9083" > /dev/null 2>&1; then
    echo "HMS ready"; break
  fi
  sleep 5
done

echo "=== Waiting for HS2 thrift port 10000 ==="
for i in $(seq 1 90); do
  if hdfs_as_nn bash -c "exec 3<>/dev/tcp/hiveserver2/10000" > /dev/null 2>&1; then
    echo "HS2 ready"; break
  fi
  sleep 5
done

TEST_DB="oauthhive_smoketest"
TEST_TABLE="greetings"
WAREHOUSE="hdfs://namenode:9000/warehouse"

beeline_exec() {
  # Connect as testuser so HiveServer2's doAs path kicks in
  # (hive.server2.enable.doAs=true) — HMS then proxies the
  # operation to the NameNode as testuser via hadoop.proxyuser.hms.*.
  docker-compose exec -T -e HOME=/tmp hiveserver2 \
    /opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/default" \
    -n testuser \
    --silent=true -e "$1" 2>&1 | tail -5
}

echo "=== CREATE DATABASE on HDFS-backed warehouse ==="
beeline_exec "DROP DATABASE IF EXISTS ${TEST_DB} CASCADE; CREATE DATABASE ${TEST_DB} LOCATION '${WAREHOUSE}/${TEST_DB}.db';"
echo "=== CREATE TABLE ==="
beeline_exec "USE ${TEST_DB}; CREATE TABLE ${TEST_TABLE} (msg STRING) STORED AS TEXTFILE;"

# HMS creates the db/table dirs as its own identity (hms, mode 755)
# while the INSERT below runs doAs the connecting user (testuser).
# On Ozone this passed because ozone.administrators=* made every
# authenticated user an admin; plain HDFS actually enforces POSIX
# perms, so open up the freshly created dirs. The data files written
# by the INSERT still get testuser as owner — that is the doAs proof.
hdfs_as_nn hdfs dfs -chmod -R 777 "/warehouse/${TEST_DB}.db"

echo "=== INSERT three rows via Tez ==="
beeline_exec "INSERT INTO ${TEST_DB}.${TEST_TABLE} VALUES ('hello-oauth'), ('hello-tez'), ('hello-hdfs');"
echo "=== COUNT(*) via multi-vertex Tez DAG ==="
COUNT_OUT=$(beeline_exec "SELECT COUNT(*) AS n FROM ${TEST_DB}.${TEST_TABLE};")
echo "${COUNT_OUT}"
if echo "${COUNT_OUT}" | grep -qE "\| *3 *\|"; then
  pass "COUNT(*) = 3 through Tez DAG"
else
  fail "expected COUNT(*) = 3 from Tez DAG"
fi

echo "=== Asserting Tez executed at least one DAG ==="
TEZ_HITS=$(docker-compose exec -T hiveserver2 bash -c "grep -hc 'tez.TezTask' /tmp/*/hive.log 2>/dev/null | head -1" 2>/dev/null | tr -d '[:space:]')
TEZ_HITS=${TEZ_HITS:-0}
echo "  tez.TezTask log hits: ${TEZ_HITS}"
if [[ "${TEZ_HITS}" -ge 1 ]]; then
  pass "Tez drove the queries"
else
  fail "Tez did not run any DAG — engine likely fell back"
fi

echo "=== doAs check: INSERT-written data files owned by testuser ==="
owners=$(hdfs_as_nn bash -c "hdfs dfs -ls /warehouse/${TEST_DB}.db/${TEST_TABLE} 2>/dev/null" | awk '/^-/{print $3}' | sort -u | tr '\n' ' ')
if [[ "$owners" == "testuser " ]]; then
  pass "table data files owned by 'testuser' (doAs + proxyuser chain works)"
else
  fail "unexpected data-file owner(s) '${owners}' in /warehouse/${TEST_DB}.db/${TEST_TABLE}"
fi

echo "=== DT-renewal probe: 100s query outliving the 90s token TTL ==="
beeline_exec "SELECT reflect('java.lang.Thread', 'sleep', cast(100000 as bigint)) FROM ${TEST_DB}.${TEST_TABLE} LIMIT 1;" > /dev/null
REFRESH_HITS=$(docker-compose logs hiveserver2 2>&1 | grep -c "Proactively refreshed OAuth token")
if [[ "${REFRESH_HITS:-0}" -ge 1 ]]; then
  pass "proactive OAuth refresh fired during long query (hits=${REFRESH_HITS})"
else
  fail "no proactive refresh observed in HS2"
fi

echo "=== Teardown: DROP DATABASE CASCADE ==="
beeline_exec "DROP DATABASE ${TEST_DB} CASCADE;"
if hdfs_as_nn hdfs dfs -test -d "/warehouse/${TEST_DB}.db" 2>/dev/null; then
  fail "post-drop: /warehouse/${TEST_DB}.db still exists"
else
  pass "post-drop: warehouse subdir cleaned up"
fi

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit "$FAIL"
