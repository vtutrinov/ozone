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
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:oauth-hive

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# docker-compose auto-loads .env, but plain bash doesn't.
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

# Fetch postgres JDBC driver if not already present — the
# apache/hive image doesn't ship it. Bind-mounted into HMS's
# lib dir below.
POSTGRES_JDBC_VERSION=${POSTGRES_JDBC_VERSION:-42.7.4}
PG_JAR_DIR="$COMPOSE_DIR/jars"
PG_JAR="$PG_JAR_DIR/postgresql.jar"
if [[ ! -f "$PG_JAR" ]]; then
  mkdir -p "$PG_JAR_DIR"
  echo "Downloading postgres JDBC driver ${POSTGRES_JDBC_VERSION}..."
  curl -sfL -o "$PG_JAR" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
fi

start_docker_env

# Keycloak readiness — same probe as ozone-oauth/test.sh.
echo "Verifying Keycloak is accessible..."
for i in $(seq 1 30); do
  if docker-compose exec -T scm curl -sf http://keycloak:8080/realms/EXAMPLE.COM > /dev/null 2>&1; then
    echo "Keycloak is ready"
    break
  fi
  echo "  waiting for Keycloak ($i/30)..."
  sleep 5
done

# The apache/hive entrypoint runs schematool itself when the
# container starts, so we don't need an explicit init step here
# — just wait for HMS to come up.

# Wait for HMS thrift port to come up after schema init.
echo "Waiting for HMS thrift port 9083..."
for i in $(seq 1 30); do
  if docker-compose exec -T scm nc -z hms 9083 > /dev/null 2>&1; then
    echo "HMS is ready"
    break
  fi
  echo "  waiting for HMS ($i/30)..."
  sleep 5
done

# Pre-create the volume/bucket that will host the HMS warehouse
# keys. The warehouse path itself (a key inside bucket1) is created
# lazily by HMS on the first CREATE DATABASE.
echo "Pre-creating volume1/bucket1 for the HMS warehouse..."
docker-compose exec -T scm bash -c "
  ozone sh volume info /volume1 >/dev/null 2>&1 || \
    ozone sh volume create /volume1 --user hms --space-quota 100TB --namespace-quota 100
  ozone sh bucket info /volume1/bucket1 >/dev/null 2>&1 || \
    ozone sh bucket create /volume1/bucket1 --space-quota 1TB --layout fso
" 2>&1 | tail -5

# Wait for HiveServer2 — runs `beeline` against this port.
echo "Waiting for HS2 thrift port 10000..."
for i in $(seq 1 60); do
  if docker-compose exec -T scm nc -z hiveserver2 10000 > /dev/null 2>&1; then
    echo "HS2 is ready"
    break
  fi
  echo "  waiting for HS2 ($i/60)..."
  sleep 5
done

# Drive the DDL through HiveServer2 from the host so the OAuth
# identity inside HS2 is the one issuing the writes. Robot then
# just asserts the resulting state on Ozone — scm doesn't have a
# Hive JDBC client of its own.
TEST_DB="oauthhive_smoketest"
TEST_TABLE="greetings"
WAREHOUSE="ofs://om/volume1/bucket1/warehouse"

beeline_exec() {
  docker-compose exec -T -e HOME=/tmp hiveserver2 \
    /opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/default" \
    --silent=true -e "$1" 2>&1 | tail -5
}

echo "Setup: CREATE DATABASE ${TEST_DB} on Ozone-backed warehouse..."
beeline_exec "DROP DATABASE IF EXISTS ${TEST_DB} CASCADE; CREATE DATABASE ${TEST_DB} LOCATION '${WAREHOUSE}/${TEST_DB}.db';"
echo "Setup: CREATE TABLE ${TEST_DB}.${TEST_TABLE}..."
beeline_exec "USE ${TEST_DB}; CREATE TABLE ${TEST_TABLE} (msg STRING) STORED AS TEXTFILE;"
echo "Setup: INSERT three rows via Tez (engine=tez, local mode)..."
beeline_exec "INSERT INTO ${TEST_DB}.${TEST_TABLE} VALUES ('hello-oauth'), ('hello-tez'), ('hello-ofs');"
echo "Setup: aggregation query (multi-vertex Tez DAG)..."
COUNT_OUT=$(beeline_exec "SELECT COUNT(*) AS n FROM ${TEST_DB}.${TEST_TABLE};")
echo "${COUNT_OUT}"
# The aggregation must return 3 — every insert path goes through
# Tez in Hive 4 (mr engine was removed) so a correct count is also
# evidence Tez actually executed the DAG.
if ! echo "${COUNT_OUT}" | grep -qE "\| *3 *\|"; then
  echo "FAIL: expected COUNT(*) = 3 from Tez DAG"
  exit 1
fi

# Verify Tez actually drove the query (not a legacy fallback engine).
# Hive 4 logs "tez.TezTask" + a TezSession id for every Tez-executed
# query; we grep hive.log inside HS2.
echo "Asserting Tez executed at least one DAG..."
TEZ_HITS=$(docker exec ozone-oauth-hive-hiveserver2-1 grep -c "tez.TezTask" /tmp/hive/hive.log || echo 0)
echo "  tez.TezTask log hits: ${TEZ_HITS}"
if [[ "${TEZ_HITS}" -lt 1 ]]; then
  echo "FAIL: Tez did not run any DAG — engine likely fell back to MR/local."
  exit 1
fi

# Delegation-token renewal probe. The realm's accessTokenLifespan
# is 90s and the agent runs auth-token-renewal=both, so its
# proactive scheduler fires a refresh at expiresAt-30s (= +60s).
# A 100-second sleep query straddles that boundary: the SAME long
# in-process Tez task runs through one token expiry and lives only
# because the proactive refresh swapped in a fresh access token.
# If the refresh broke (or was off), the next ofs:// RPC after +90s
# would fail with "Token is not active".
echo "DT-renewal probe: 100s sleep query that outlives the access TTL..."
beeline_exec "SELECT reflect('java.lang.Thread', 'sleep', cast(100000 as bigint)) FROM ${TEST_DB}.${TEST_TABLE} LIMIT 1;"
echo "Asserting proactive refresh fired during the long query..."
REFRESH_HITS=$(docker logs ozone-oauth-hive-hiveserver2-1 2>&1 | grep -c "Proactively refreshed OAuth token" || echo 0)
echo "  proactive refresh hits in HS2: ${REFRESH_HITS}"
if [[ "${REFRESH_HITS}" -lt 1 ]]; then
  echo "FAIL: no proactive refresh observed — long-running queries " \
       "would have hit 'Token is not active'."
  exit 1
fi

# Robot tests run inside scm — they verify the Ozone-side state
# we just set up (port reachability + warehouse dirs materialised).
execute_robot_test scm -v TEST_DB:"${TEST_DB}" -v TEST_TABLE:"${TEST_TABLE}" -v WAREHOUSE:"${WAREHOUSE}" security/hms.robot

echo "Teardown: DROP DATABASE ${TEST_DB} CASCADE..."
beeline_exec "DROP DATABASE ${TEST_DB} CASCADE;"

# Final assertion: post-drop the warehouse subdir is gone.
docker-compose exec -T scm bash -c "
  if ozone fs -test -d ${WAREHOUSE}/${TEST_DB}.db 2>/dev/null; then
    echo 'POST-DROP FAIL: ${WAREHOUSE}/${TEST_DB}.db still exists'
    exit 1
  fi
  echo 'POST-DROP OK: warehouse subdir cleaned up'
"
