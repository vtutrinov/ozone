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

# E2E test for the fail-closed Ranger policy-cache TTL and the
# `ozone admin om rangercache` CLI.
#
# Prerequisites (locally built, no downloads):
#   RANGER_SOURCE_DIR       - ranger fork checkout (for the pg init script)
#   RANGER_OZONE_PLUGIN_DIR - untarred ranger-<ver>-ozone-plugin dir
#
# The ranger.yaml overlay configures expiryMs=60000, pollIntervalMs=5000.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${RANGER_VERSION:=2.4.0}"
: "${RANGER_SOURCE_DIR:=/work/sber/component-ranger-plugins}"
: "${RANGER_OZONE_PLUGIN_DIR:?set RANGER_OZONE_PLUGIN_DIR to the untarred plugin dir}"

export RANGER_VERSION RANGER_SOURCE_DIR RANGER_OZONE_PLUGIN_DIR
export RANGER_DB_IMAGE=postgres
export RANGER_DB_IMAGE_VERSION=12
# apache/ranger images on Docker Hub are arm64-only; use the locally built
# image (dev-support/ranger-docker, Dockerfile.ranger) on x86_64
export RANGER_IMAGE="${RANGER_IMAGE:-ranger-local}"
export RANGER_IMAGE_VERSION="${RANGER_IMAGE_VERSION:-${RANGER_VERSION}}"

export COMPOSE_FILE=docker-compose.yaml:ranger.yaml:../common/ranger.yaml
export OM_SERVICE_ID="omservice"
export SECURITY_ENABLED=true

RANGER_URL="http://localhost:6080"
RANGER_AUTH="admin:rangerR0cks!"
TEST_VOL="vol$(date +%s)"
TTL_MS=60000
POLL_MS=5000

FAILURES=0

cd "$COMPOSE_DIR"

step()  { echo; echo "=== $*"; }
check() { # check <description> <command...>
  local desc="$1"; shift
  if "$@" > /tmp/check-out.log 2>&1; then
    echo "PASS: $desc"
  else
    echo "FAIL: $desc"; tail -5 /tmp/check-out.log | sed 's/^/    /'
    FAILURES=$((FAILURES+1))
  fi
}
check_fails() { # check_fails <description> <command...> - expect NON-zero
  local desc="$1"; shift
  if "$@" > /tmp/check-out.log 2>&1; then
    echo "FAIL (expected denial): $desc"; tail -5 /tmp/check-out.log | sed 's/^/    /'
    FAILURES=$((FAILURES+1))
  else
    echo "PASS (denied as expected): $desc"
  fi
}

om_exec() { docker compose exec -T om1 "$@"; }

as_testuser() { # run an ozone command as kinit'ed testuser on om1
  # per-invocation credential cache: concurrent kinit's against the default
  # ccache race with each other and fail transiently
  om_exec bash -c "export KRB5CCNAME=/tmp/krb5cc-test-\$\$; kinit -k -t /etc/security/keytabs/testuser.keytab testuser/\$(hostname | sed 's/scm[0-9].org/scm/;s/scm[0-9]/scm/;s/om[0-9]/om/')@EXAMPLE.COM && $*"
}

rangercache() {
  as_testuser "ozone admin om rangercache -id omservice $*"
}

wait_for_state() { # wait_for_state <state> <timeout-seconds> - real-time based
  local state="$1" timeout="$2" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    rangercache --status > /tmp/wait-probe.log 2>&1
    if grep -q "\"state\":\"$state\"" /tmp/wait-probe.log; then
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for cache state $state; last probe output:"
  tail -6 /tmp/wait-probe.log
  return 1
}

step "Starting docker environment"
docker compose up -d
echo "waiting for ranger admin (initial setup takes a few minutes)..."
timeout 600 bash -c "until curl -sf -o /dev/null $RANGER_URL/login.jsp; do sleep 5; done" || exit 1

step "Creating dev_ozone service and an allow-all test policy in Ranger"
curl -sf -u "$RANGER_AUTH" -H 'Content-Type: application/json' \
  -X POST "$RANGER_URL/service/public/v2/api/service" -d '{
    "name": "dev_ozone",
    "type": "ozone",
    "configs": {
      "username": "admin",
      "password": "rangerR0cks!",
      "ozone.om.http-address": "http://om1:9874",
      "hadoop.security.authentication": "kerberos",
      "hadoop.security.authorization": "true",
      "commonNameForCertificate": ""
    }
  }' > /dev/null || echo "(service may already exist)"

curl -sf -u "$RANGER_AUTH" -H 'Content-Type: application/json' \
  -X POST "$RANGER_URL/service/public/v2/api/policy" -d '{
    "service": "dev_ozone",
    "name": "test-allow-all",
    "resources": {
      "volume": {"values": ["*"]},
      "bucket": {"values": ["*"]},
      "key":    {"values": ["*"]}
    },
    "policyItems": [{
      "users": ["testuser", "om", "hdfs", "scm", "s3g", "recon", "httpfs", "hadoop"],
      "accesses": [
        {"type": "read", "isAllowed": true}, {"type": "write", "isAllowed": true},
        {"type": "create", "isAllowed": true}, {"type": "list", "isAllowed": true},
        {"type": "delete", "isAllowed": true}, {"type": "read_acl", "isAllowed": true},
        {"type": "write_acl", "isAllowed": true}
      ],
      "delegateAdmin": true
    }]
  }' > /dev/null || echo "(policy may already exist)"

step "Waiting for OMs to come up and sync policies"
timeout 300 bash -c 'until docker compose exec -T om1 bash -c "kinit -k -t /etc/security/keytabs/testuser.keytab testuser/om@EXAMPLE.COM && ozone admin om roles -id=omservice" 2>/dev/null | grep -qi leader; do sleep 5; done' \
  || { echo "OMs did not form a quorum"; docker compose ps; exit 1; }
sleep $((POLL_MS / 1000 + 5))

step "1. Sanity: data path works while Ranger is up"
check "volume create" as_testuser "ozone sh volume create /${TEST_VOL}"
check "bucket create" as_testuser "ozone sh bucket create /${TEST_VOL}/bucket1"
check "key put" as_testuser "echo hello > /tmp/k1 && ozone sh key put /${TEST_VOL}/bucket1/k1 /tmp/k1"

step "2. rangercache --status shows FRESH on all OMs"
rangercache --status
check "status FRESH" wait_for_state FRESH 90

step "3. Stop Ranger; within TTL access still allowed"
docker compose stop ranger ranger-db
sleep 10
check "key put within TTL" as_testuser "echo hi > /tmp/k2 && ozone sh key put /${TEST_VOL}/bucket1/k2 /tmp/k2"

step "4. After TTL expiry all access is denied (fail-closed)"
check "status EXPIRED" wait_for_state EXPIRED $((TTL_MS / 1000 + 60))
check_fails "key put after TTL" as_testuser "echo hi > /tmp/k3 && ozone sh key put /${TEST_VOL}/bucket1/k3 /tmp/k3"
check_fails "key get after TTL" as_testuser "ozone sh key get /${TEST_VOL}/bucket1/k1 /tmp/k1.out"

step "5. --extend revives the cache"
rangercache --extend --ttl 3m
check "status EXTENDED" wait_for_state EXTENDED 90
check "key put after extend" as_testuser "echo hi > /tmp/k4 && ozone sh key put /${TEST_VOL}/bucket1/k4 /tmp/k4"

step "6. Restart Ranger; cache returns to FRESH"
docker compose start ranger-db ranger
timeout 300 bash -c "until curl -sf -o /dev/null $RANGER_URL/login.jsp; do sleep 5; done"
check "status FRESH after ranger restart" wait_for_state FRESH 120
check "key put after recovery" as_testuser "echo hi > /tmp/k5 && ozone sh key put /${TEST_VOL}/bucket1/k5 /tmp/k5"

step "7. Explicit --invalidate denies, auto-heals after next successful sync"
docker compose pause ranger
rangercache --invalidate || true
check_fails "key put right after invalidate" as_testuser "ozone sh key put /${TEST_VOL}/bucket1/k6 /tmp/k1"
docker compose unpause ranger
sleep $((POLL_MS / 1000 + 10))
check "key put after auto-heal" as_testuser "echo hi > /tmp/k7 && ozone sh key put /${TEST_VOL}/bucket1/k7 /tmp/k7"

step "8. Per-policy invalidation (Ranger paused so the next sync cannot heal it mid-test)"
docker compose pause ranger
rangercache --invalidate --policy test-allow-all || true
check_fails "key put with policy invalidated" as_testuser "ozone sh key put /${TEST_VOL}/bucket1/k8 /tmp/k1"
docker compose unpause ranger
sleep $((POLL_MS / 1000 + 10))
check "key put after policy restored by sync" as_testuser "echo hi > /tmp/k9 && ozone sh key put /${TEST_VOL}/bucket1/k9 /tmp/k9"

step "9. Single-node targeting (-host om2 affects only om2)"
docker compose pause ranger
rangercache --invalidate -host om2 || true
STATUS_OUT="$(rangercache --status || true)"
echo "$STATUS_OUT"
INVALIDATED_COUNT=$(grep -o '"state":"INVALIDATED"' <<< "$STATUS_OUT" | wc -l)
if [[ "$INVALIDATED_COUNT" == "1" ]] && grep -E '^om2\b.*INVALIDATED' <<< "$STATUS_OUT" > /dev/null 2>&1; then
  echo "PASS: om2 (and only om2) is INVALIDATED"
else
  echo "FAIL: expected exactly om2 INVALIDATED; got $INVALIDATED_COUNT invalidated node(s):"
  grep INVALIDATED <<< "$STATUS_OUT" | sed 's/^/    /'
  FAILURES=$((FAILURES+1))
fi
docker compose unpause ranger

wait_for_no_invalidated() { # all nodes healed
  local start=$SECONDS
  while (( SECONDS - start < 60 )); do
    if ! rangercache --status 2>/dev/null | grep -q '"state":"INVALIDATED"'; then
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for INVALIDATED to clear"
  return 1
}
check "om2 auto-heals after Ranger resumes" wait_for_no_invalidated

echo
if (( FAILURES > 0 )); then
  echo "E2E FAILED: $FAILURES check(s) failed"
  exit 1
fi
echo "E2E PASSED"
