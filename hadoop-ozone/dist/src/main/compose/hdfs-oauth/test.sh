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

#suite:oauth-hdfs

# Unlike the ozone-oauth* suites this one is self-contained bash:
# it needs no Ozone dist tarball and no robot framework — the whole
# cluster is stock Apache Hadoop + Keycloak + the agent jar.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$COMPOSE_DIR" || exit 1

FAIL=0
fail() { echo "FAIL: $*"; FAIL=1; }
pass() { echo "PASS: $*"; }

# Per-exec identity override: the agent's env AuthDataProvider reads
# AUTH_LOGIN/AUTH_PASSWORD from the process env, so `docker-compose
# exec -e` lets one container act as different OAuth users.
# OZONE_AGENT_LOG_LEVEL=OFF keeps agent chatter out of stdout, which
# tests compare byte-for-byte (cat/stat round-trips).
as_user() {
  local user="$1"; local svc="$2"; shift 2
  docker-compose exec -T -e AUTH_LOGIN="$user" -e AUTH_PASSWORD="$user" \
    -e OZONE_AGENT_LOG_LEVEL=OFF "$svc" "$@"
}

# grep -q would SIGPIPE docker-compose under `set -o pipefail` and
# turn a successful match into a failed pipeline — drain stdin fully.
log_contains() {
  docker-compose logs "$1" 2>&1 | grep -c "$2" > /dev/null
}

if [[ "${1:-}" != "--no-recreate" ]]; then
  docker-compose down -v 2>/dev/null
  docker-compose up -d
fi

echo "=== Waiting for Keycloak realm ==="
for i in $(seq 1 30); do
  if docker-compose exec -T namenode curl -sf http://keycloak:8080/realms/EXAMPLE.COM > /dev/null 2>&1; then
    echo "Keycloak ready"; break
  fi
  sleep 5
done

echo "=== Waiting for HDFS (NN out of safemode, 1 live DN) ==="
hdfs_ready=false
for i in $(seq 1 60); do
  live=$(docker-compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -c "^Name:")
  if [[ "${live:-0}" -ge 1 ]]; then
    docker-compose exec -T namenode hdfs dfsadmin -safemode wait > /dev/null 2>&1
    hdfs_ready=true; echo "HDFS ready ($live live datanode(s))"; break
  fi
  sleep 5
done
$hdfs_ready || { fail "HDFS did not come up"; docker-compose logs --tail 30 namenode datanode; exit 1; }

echo "=== Verifying agent interception (no KDC, no keytabs) ==="
for svc in namenode datanode rm nm; do
  if log_contains "$svc" "Installed Hadoop security auth agent"; then
    pass "agent loaded on $svc"
  else
    fail "agent not detected on $svc"
  fi
done
if log_contains namenode "auth:OAUTH"; then
  pass "NameNode authenticates callers as auth:OAUTH"
else
  fail "no auth:OAUTH authentication seen on namenode"
fi

echo "=== HDFS write/read as user hadoop ==="
docker-compose exec -T -e OZONE_AGENT_LOG_LEVEL=OFF namenode bash -c "
  hdfs dfs -mkdir -p /user/hadoop /user/testuser /tmp &&
  hdfs dfs -chown hadoop /user/hadoop &&
  hdfs dfs -chown testuser /user/testuser &&
  hdfs dfs -chmod 1777 /tmp
" || fail "superuser (nn) could not prepare /user and /tmp dirs"

as_user hadoop datanode bash -c "
  set -e
  printf 'hello secure hdfs over oauth\n%s\n' \"\$(seq 1 200)\" > /tmp/probe.txt
  hdfs dfs -copyFromLocal -f /tmp/probe.txt /user/hadoop/probe.txt
  hdfs dfs -cat /user/hadoop/probe.txt > /tmp/probe-back.txt
  cmp /tmp/probe.txt /tmp/probe-back.txt
" && pass "hdfs write+read-back as 'hadoop' (round-trip identical)" \
  || fail "hdfs write/read as 'hadoop'"

echo "=== Ownership / identity check ==="
owner=$(as_user hadoop datanode hdfs dfs -stat '%u' /user/hadoop/probe.txt 2>/dev/null | tr -d '[:space:]')
if [[ "$owner" == "hadoop" ]]; then
  pass "file owner is 'hadoop' (OAuth identity mapped to HDFS user)"
else
  fail "unexpected owner '$owner' for /user/hadoop/probe.txt"
fi

echo "=== Permission enforcement across OAuth identities ==="
if as_user testuser datanode hdfs dfs -rm -skipTrash /user/hadoop/probe.txt > /dev/null 2>&1; then
  fail "testuser could delete hadoop's file (permissions not enforced)"
else
  pass "testuser denied deleting hadoop's file"
fi

echo "=== MapReduce: pi over YARN ==="
EXAMPLES_JAR=$(docker-compose exec -T rm bash -c "ls /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar" | tr -d '[:space:]')
as_user hadoop rm yarn jar "$EXAMPLES_JAR" pi 2 4 > /tmp/hdfs-oauth-pi.out 2>&1
if grep -q "Estimated value of Pi" /tmp/hdfs-oauth-pi.out; then
  pass "MR pi job completed"
else
  fail "MR pi job did not complete"; tail -20 /tmp/hdfs-oauth-pi.out
fi

echo "=== MapReduce: wordcount over YARN, output on hdfs:// ==="
as_user hadoop rm bash -c "
  set -e
  printf 'apache hadoop secure oauth\nhadoop hdfs oauth agent\nagent intercepts kerberos\n' > /tmp/wc-in.txt
  hdfs dfs -mkdir -p /user/hadoop/wc-in
  hdfs dfs -copyFromLocal -f /tmp/wc-in.txt /user/hadoop/wc-in/
  hdfs dfs -rm -r -f -skipTrash /user/hadoop/wc-out
" || fail "wordcount input staging"
as_user hadoop rm yarn jar "$EXAMPLES_JAR" wordcount /user/hadoop/wc-in /user/hadoop/wc-out > /tmp/hdfs-oauth-wc.out 2>&1
wc_hadoop=$(as_user hadoop rm hdfs dfs -cat '/user/hadoop/wc-out/part-r-*' 2>/dev/null | awk '$1=="hadoop"{print $2}')
if [[ "$wc_hadoop" == "2" ]]; then
  pass "MR wordcount completed with correct counts (hadoop=2)"
else
  fail "MR wordcount bad output (hadoop='$wc_hadoop')"; tail -20 /tmp/hdfs-oauth-wc.out
fi

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit "$FAIL"
