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

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true

start_docker_env

# Keycloak readiness is handled by Docker healthcheck + depends_on,
# but verify from test runner side too
echo "Verifying Keycloak is accessible..."
for i in $(seq 1 30); do
  if docker-compose exec -T scm curl -sf http://keycloak:8080/realms/ozone > /dev/null 2>&1; then
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

# Run OAuth-specific acceptance tests
execute_robot_test scm security/ozone-oauth.robot

# Run basic Ozone operations — these require working auth
# In a secure cluster without KDC, these ONLY work if the
# agent successfully replaced Kerberos with OAuth
execute_robot_test scm basic
