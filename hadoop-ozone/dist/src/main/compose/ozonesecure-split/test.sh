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

#suite:secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true

start_docker_env

# Sanity: bring an external client through Kerberos against the cluster.
# Run from `httpfs` — the external-client container — NOT from inside a
# daemon (scm/om), so that the test exercises the same Kerberos path a real
# end-user would take: client outside the Ozone JVMs, talking to OM's
# external client port (Kerberos) and to S3G's HTTP listener (Sig V4).
execute_robot_test httpfs kinit.robot

# Verify ofs + S3 work end-to-end from the external client, even though the
# OM service-RPC port and the SCM↔DN heartbeat path run SIMPLE auth.
execute_robot_test httpfs security/split-kerberos.robot

# Validate the operator-facing WARN from OzoneSecurityUtil.validateKerberosFlags.
# This runs from the host (Robot can't see OM's stdout from inside SCM).
om_container=$(docker ps --filter "label=com.docker.compose.service=om" \
  --filter "label=com.docker.compose.project=$(basename "$COMPOSE_DIR")" \
  --format '{{.Names}}' | head -1)
if [[ -z "$om_container" ]] || \
   ! docker logs "$om_container" 2>&1 | grep -q "split-Kerberos mode (external=true, interservice=false)"; then
  echo "FAIL: OM did not emit the split-Kerberos advisory WARN" >&2
  exit 1
fi
echo "PASS: OM advisory WARN observed in container ${om_container}"

# Existing security suite re-run as a regression check: nothing should
# regress when interservice Kerberos is off but external is on.
execute_robot_test scm security
execute_robot_test scm basic
