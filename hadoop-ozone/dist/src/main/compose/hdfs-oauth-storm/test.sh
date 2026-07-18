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

#suite:oauth-hdfs-storm

# Self-contained bash suite — no Ozone dist, no robot. Storage is
# plain HDFS; the checks mirror ozone-oauth-storm/test.sh.

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

echo "=== Preparing /storm-test workspace on HDFS ==="
hdfs_as_nn bash -c "
  hdfs dfs -mkdir -p /storm-test /user/storm &&
  hdfs dfs -chown -R storm /storm-test /user/storm
" || fail "could not prepare HDFS dirs"

echo "=== Waiting for Nimbus thrift port 6627 ==="
for i in $(seq 1 60); do
  if hdfs_as_nn bash -c "exec 3<>/dev/tcp/nimbus/6627" > /dev/null 2>&1; then
    echo "nimbus ready"; break
  fi
  sleep 5
done

echo "=== Agent loaded on Storm daemons ==="
for svc in nimbus supervisor; do
  if docker-compose logs "$svc" 2>&1 | grep -c "Installed Hadoop security auth agent" > /dev/null; then
    pass "agent loaded on $svc"
  else
    fail "agent not detected on $svc"
  fi
done

# End-to-end write from within the Nimbus container to hdfs://.
# This is the Storm-container equivalent of what a topology's
# HdfsBolt would do: the JVM gets the agent via JAVA_TOOL_OPTIONS,
# the hdfs client classes from the hadooplibs volume, and OAuth
# credentials (AUTH_LOGIN=storm) from the container env — the same
# auth + write pipeline a scheduled bolt would use.
WORKSPACE="hdfs://namenode:9000/storm-test"
PROBE_KEY="${WORKSPACE}/probe.txt"
echo "=== Writing probe from Nimbus JVM as user 'storm' ==="
docker-compose exec -T -e OZONE_AGENT_LOG_LEVEL=OFF nimbus bash -c "
  echo storm-oauth-probe > /tmp/probe.txt
  java -cp '/opt/hadoop-conf:/opt/hadooplibs/*' org.apache.hadoop.fs.FsShell -put -f /tmp/probe.txt ${PROBE_KEY}
" > /tmp/storm-put.out 2>&1
if grep -qiE "exception|error" /tmp/storm-put.out; then
  fail "FsShell put from Nimbus reported errors"; tail -10 /tmp/storm-put.out
else
  pass "probe written from Nimbus JVM"
fi

echo "=== Verifying probe on HDFS: content + owner ==="
content=$(hdfs_as_nn hdfs dfs -cat /storm-test/probe.txt 2>/dev/null | tr -d '[:space:]')
owner=$(hdfs_as_nn hdfs dfs -stat '%u' /storm-test/probe.txt 2>/dev/null | tr -d '[:space:]')
if [[ "$content" == "storm-oauth-probe" ]]; then
  pass "probe content round-trips"
else
  fail "probe content mismatch: '$content'"
fi
if [[ "$owner" == "storm" ]]; then
  pass "probe owned by 'storm' (OAuth identity mapped)"
else
  fail "unexpected probe owner '$owner'"
fi

echo "=== Teardown ==="
hdfs_as_nn hdfs dfs -rm -r -f -skipTrash /storm-test > /dev/null 2>&1
if hdfs_as_nn hdfs dfs -test -d /storm-test 2>/dev/null; then
  fail "post-drop: /storm-test still exists"
else
  pass "post-drop: storm workspace cleaned up"
fi

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit "$FAIL"
