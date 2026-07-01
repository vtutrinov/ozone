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

#suite:oauth-flink

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

echo "Pre-creating volume1/bucket1 for the Flink workspace..."
docker-compose exec -T scm bash -c "
  ozone sh volume info /volume1 >/dev/null 2>&1 || \
    ozone sh volume create /volume1 --user flink --space-quota 100TB --namespace-quota 100
  ozone sh bucket info /volume1/bucket1 >/dev/null 2>&1 || \
    ozone sh bucket create /volume1/bucket1 --space-quota 1TB --layout fso
" 2>&1 | tail -3

echo "Waiting for Flink JobManager REST port 8081 (inside container)..."
for i in $(seq 1 60); do
  if docker-compose exec -T scm nc -z jobmanager 8081 > /dev/null 2>&1; then
    echo "jobmanager is ready"
    break
  fi
  sleep 5
done

echo "Waiting for TaskManager to register..."
for i in $(seq 1 60); do
  if docker-compose logs jobmanager 2>&1 | grep -q "Registering TaskManager"; then
    echo "taskmanager registered"
    break
  fi
  sleep 5
done

# Stage a small text file on ofs:// so the batch WordCount has
# something to read.
WORKSPACE="ofs://om/volume1/bucket1/flink-test"
INPUT_KEY="${WORKSPACE}/input.txt"
OUTPUT_KEY="${WORKSPACE}/wc-output"
echo "Staging input.txt on ${INPUT_KEY}..."
docker-compose exec -T scm bash -c "
  printf 'apache ozone apache flink\nozone secure oauth flink\nflink streams ozone keys\n' > /tmp/wc-input.txt
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -mkdir -p ${WORKSPACE} 2>&1 | tail -1 || true
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -copyFromLocal -f /tmp/wc-input.txt ${INPUT_KEY}
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -rm -r -f -skipTrash ${OUTPUT_KEY} 2>&1 | tail -1 || true
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -test -e ${INPUT_KEY} && echo 'input staged'
" 2>&1 | tail -3

echo "Submitting Flink batch WordCount..."
docker-compose exec -T jobmanager \
  /opt/flink/bin/flink run \
    --jobmanager jobmanager:8081 \
    /opt/flink/examples/batch/WordCount.jar \
    --input "${INPUT_KEY}" \
    --output "${OUTPUT_KEY}" > /tmp/flink-wc.out 2>&1 || true
tail -5 /tmp/flink-wc.out
if ! grep -qiE "Program execution finished" /tmp/flink-wc.out; then
  echo "FAIL: Flink WordCount did not report success"
  exit 1
fi

# Robot tests run inside scm — they verify the Ozone-side state.
execute_robot_test scm -v WORKSPACE:"${WORKSPACE}" security/flink.robot

echo "Teardown: drop the flink-test workspace..."
docker-compose exec -T scm bash -c "
  OZONE_AGENT_LOG_LEVEL=OFF ozone fs -rm -r -f -skipTrash ${WORKSPACE} 2>&1 | tail -1
"
docker-compose exec -T scm bash -c "
  if ozone fs -test -d ${WORKSPACE} 2>/dev/null; then
    echo 'POST-DROP FAIL: ${WORKSPACE} still exists'
    exit 1
  else
    echo 'POST-DROP OK: flink workspace cleaned up'
  fi
"
