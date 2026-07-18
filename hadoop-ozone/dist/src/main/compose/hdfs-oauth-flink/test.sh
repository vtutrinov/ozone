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

#suite:oauth-hdfs-flink

# Self-contained bash suite — no Ozone dist, no robot. Storage is
# plain HDFS; the checks mirror ozone-oauth-flink/test.sh.

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

echo "=== Preparing /flink-test workspace on HDFS ==="
hdfs_as_nn bash -c "
  hdfs dfs -mkdir -p /flink-test /user/flink &&
  hdfs dfs -chown -R flink /flink-test /user/flink
" || fail "could not prepare HDFS dirs"

echo "=== Waiting for Flink JobManager REST port 8081 ==="
for i in $(seq 1 60); do
  if hdfs_as_nn bash -c "exec 3<>/dev/tcp/jobmanager/8081" > /dev/null 2>&1; then
    echo "jobmanager ready"; break
  fi
  sleep 5
done

echo "=== Waiting for TaskManager to register ==="
for i in $(seq 1 60); do
  if docker-compose logs jobmanager 2>&1 | grep -c "Registering TaskManager" > /dev/null; then
    echo "taskmanager registered"; break
  fi
  sleep 5
done

echo "=== Agent loaded on Flink daemons ==="
for svc in jobmanager taskmanager; do
  if docker-compose logs "$svc" 2>&1 | grep -c "Installed Hadoop security auth agent" > /dev/null; then
    pass "agent loaded on $svc"
  else
    fail "agent not detected on $svc"
  fi
done

WORKSPACE="hdfs://namenode:9000/flink-test"
INPUT_KEY="${WORKSPACE}/input.txt"
OUTPUT_KEY="${WORKSPACE}/wc-output"

echo "=== Staging WordCount input on hdfs:// ==="
hdfs_as_nn bash -c "
  printf 'apache hadoop apache flink\nhdfs secure oauth flink\nflink streams hdfs files\n' > /tmp/wc-input.txt
  hdfs dfs -copyFromLocal -f /tmp/wc-input.txt ${INPUT_KEY}
  hdfs dfs -chown flink ${INPUT_KEY}
  hdfs dfs -rm -r -f -skipTrash ${OUTPUT_KEY} > /dev/null 2>&1 || true
  hdfs dfs -test -e ${INPUT_KEY}
" && echo "input staged" || fail "could not stage input"

echo "=== Submitting Flink batch WordCount (hdfs -> hdfs) ==="
docker-compose exec -T jobmanager \
  /opt/flink/bin/flink run \
    --jobmanager jobmanager:8081 \
    /opt/flink/examples/batch/WordCount.jar \
    --input "${INPUT_KEY}" \
    --output "${OUTPUT_KEY}" > /tmp/flink-wc.out 2>&1
if grep -qiE "Program execution finished" /tmp/flink-wc.out; then
  pass "Flink WordCount finished"
else
  fail "Flink WordCount did not report success"; tail -10 /tmp/flink-wc.out
fi

echo "=== Verifying WordCount output counts ==="
wc_flink=$(hdfs_as_nn bash -c "hdfs dfs -cat ${OUTPUT_KEY}/* 2>/dev/null" | awk '$1=="flink"{print $2}')
if [[ "$wc_flink" == "3" ]]; then
  pass "output on hdfs:// has correct counts (flink=3)"
else
  fail "bad WordCount output (flink='$wc_flink')"
fi

echo "=== Output files owned by 'flink' (OAuth identity) ==="
owners=$(hdfs_as_nn bash -c "hdfs dfs -ls ${OUTPUT_KEY} 2>/dev/null" | awk '/^[-d]/{print $3}' | sort -u | tr '\n' ' ')
if [[ "$owners" == "flink " ]]; then
  pass "output owned by 'flink'"
else
  fail "unexpected output owner(s): '$owners'"
fi

echo "=== Teardown ==="
hdfs_as_nn hdfs dfs -rm -r -f -skipTrash /flink-test > /dev/null 2>&1
if hdfs_as_nn hdfs dfs -test -d /flink-test 2>/dev/null; then
  fail "post-drop: /flink-test still exists"
else
  pass "post-drop: flink workspace cleaned up"
fi

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit "$FAIL"
