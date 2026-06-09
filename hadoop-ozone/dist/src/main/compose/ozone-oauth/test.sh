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

#suite:oauth

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# docker-compose reads .env automatically, but plain bash doesn't.
# common/hadoop-test.sh reads HADOOP_IMAGE / HADOOP_VERSION from the
# shell env to build HADOOP_TEST_IMAGE; without sourcing .env first
# it falls into an unfiltered Maven default (${docker.hadoop.image})
# that aborts the script with "bad substitution".
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

# Keycloak readiness is handled by Docker healthcheck + depends_on,
# but verify from test runner side too
echo "Verifying Keycloak is accessible..."
for i in $(seq 1 30); do
  if docker-compose exec -T scm curl -sf http://keycloak:8080/realms/EXAMPLE.COM > /dev/null 2>&1; then
    echo "Keycloak is ready"
    break
  fi
  echo "  waiting for Keycloak ($i/30)..."
  sleep 5
done

# Verify agent is loaded on each service
echo "Verifying agent installation..."
for svc in scm om datanode s3g recon; do
  if docker-compose logs "$svc" 2>&1 | grep -q "Installed Hadoop security auth agent"; then
    echo "  Agent loaded on $svc"
  else
    echo "  WARNING: Agent not detected on $svc"
  fi
done

# Verify OAuth token acquisition worked (proves no KDC needed)
echo "Verifying OAuth token acquisition..."
for svc in scm om; do
  if docker-compose logs "$svc" 2>&1 | grep -q "OAuth token obtained"; then
    echo "  OAuth token obtained on $svc"
  else
    echo "  WARNING: No OAuth token log on $svc"
  fi
done

# Run OAuth-specific acceptance tests. These exercise the same
# operations the shared `basic` smoketest would (volume / bucket /
# key create, list, read-back), but with OAuth-aware identity and
# silenced agent stdout. We deliberately don't run the shared
# `basic` suite here: it's hardwired to Kerberos (Kinit test user
# <user> <keytab>), and our cluster runs no KDC and ships no
# keytabs, so every basic test would retry kinit for two minutes
# before failing. The OAuth suite is the equivalent for this
# deployment.
execute_robot_test scm security/ozone-oauth.robot

# YARN/MR end-to-end (pi job). hadoop-test.sh brings up rm/nm/jhs
# via hadoop-secure-oauth.yaml (the OAuth compose overlay selected
# by SECURITY_OAUTH_ENABLED above) and skips kinit-only suites in
# that mode. mapreduce.robot does not itself kinit — auth is via
# the agent's -javaagent on the rm container's HADOOP_OPTS.
#
# Restrict the per-image loop to Hadoop 3.x. The agent's ByteBuddy
# interceptors target Hadoop 3.x UGI signatures; Hadoop 2.10.2's
# RM/NM containers don't get their Kerberos keytab login intercepted
# and crash at startup with "Unable to obtain password from user".
export HADOOP_TEST_IMAGES="${HADOOP_IMAGE}:3.3.6 ${HADOOP_IMAGE}:${HADOOP_VERSION}"
source "$COMPOSE_DIR/../common/hadoop-test.sh"
