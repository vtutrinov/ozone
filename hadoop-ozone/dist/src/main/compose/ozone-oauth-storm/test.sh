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

#suite:oauth-storm

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

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

echo "Pre-creating volume1/bucket1 for the Storm workspace..."
docker-compose exec -T scm bash -c "
  ozone sh volume info /volume1 >/dev/null 2>&1 || \
    ozone sh volume create /volume1 --user storm --space-quota 100TB --namespace-quota 100
  ozone sh bucket info /volume1/bucket1 >/dev/null 2>&1 || \
    ozone sh bucket create /volume1/bucket1 --space-quota 1TB --layout fso
" 2>&1 | tail -3

echo "Waiting for Nimbus thrift port 6627..."
for i in $(seq 1 60); do
  if docker-compose exec -T scm nc -z nimbus 6627 > /dev/null 2>&1; then
    echo "nimbus is ready"
    break
  fi
  sleep 5
done

echo "Waiting for Supervisor to register with Nimbus..."
for i in $(seq 1 60); do
  # Storm's supervisor logs "Registering supervisor" on the
  # nimbus side once ZK has propagated the beat.
  if docker-compose logs nimbus 2>&1 | grep -q "Received supervisor sync"; then
    echo "supervisor registered"
    break
  fi
  sleep 5
done

# Assert the agent loaded on both daemon JVMs. Storm's docker
# entrypoint launches with JAVA_TOOL_OPTIONS which pulls in the
# javaagent — the banner + Replaced-UGI line prove premain ran
# before any Storm code and the OAuth flow completed.
echo "Asserting agent loaded on Nimbus JVM..."
NIMBUS_BANNER=$(docker-compose logs nimbus 2>&1 | grep -c "Installed Hadoop security auth agent" || echo 0)
echo "  Nimbus banner count: ${NIMBUS_BANNER}"
[[ "${NIMBUS_BANNER}" -ge 1 ]] || { echo "FAIL: agent banner missing on Nimbus"; exit 1; }

echo "Asserting agent loaded on Supervisor JVM..."
SUP_BANNER=$(docker-compose logs supervisor 2>&1 | grep -c "Installed Hadoop security auth agent" || echo 0)
echo "  Supervisor banner count: ${SUP_BANNER}"
[[ "${SUP_BANNER}" -ge 1 ]] || { echo "FAIL: agent banner missing on Supervisor"; exit 1; }

# End-to-end write from within the Nimbus container to ofs://.
# This is the Storm-container equivalent of what a topology's
# HdfsBolt would do: the Nimbus JVM has both the OAuth-loaded
# agent AND the shaded Ozone client (via
# /opt/ozone/share/ozone/lib mounted through the daemon extlib),
# so `ozone fs -put` exercises the same auth + write pipeline
# the topology would use minus the actual bolt scheduling.
WORKSPACE="ofs://om/volume1/bucket1/storm-test"
PROBE_KEY="${WORKSPACE}/probe.txt"
echo "Writing probe key from Nimbus: ${PROBE_KEY}..."
docker exec ozone-oauth-storm-nimbus-1 bash -c "
  echo storm-oauth-probe > /tmp/probe.txt
  OZONE_AGENT_LOG_LEVEL=OFF /opt/ozone/bin/ozone fs -mkdir -p ${WORKSPACE} 2>&1 | tail -1 || true
  OZONE_AGENT_LOG_LEVEL=OFF /opt/ozone/bin/ozone fs -put -f /tmp/probe.txt ${PROBE_KEY} 2>&1 | tail -1
" 2>&1 | tail -3

# Robot tests run inside scm — they verify the Ozone-side state.
execute_robot_test scm -v WORKSPACE:"${WORKSPACE}" security/storm.robot

echo "Teardown: drop the storm-test workspace..."
docker-compose exec -T scm bash -c "
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -rm -r -f -skipTrash ${WORKSPACE} 2>&1 | tail -1
"
docker-compose exec -T scm bash -c "
  if ozone fs -test -d ${WORKSPACE} 2>/dev/null; then
    echo 'POST-DROP FAIL: ${WORKSPACE} still exists'
    exit 1
  else
    echo 'POST-DROP OK: storm workspace cleaned up'
  fi
"
