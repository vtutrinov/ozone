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
# OM Kerberos principal migration: per-pod _HOST  →  stable om/<service>@REALM.
#
# Use this when the OM is fronted by a load balancer (e.g. MetalLB VIP) and
# clients are hitting "Server has invalid Kerberos principal: om/<pod-fqdn>@…,
# expecting: om/<lb-ip>@…" because their SPN derivation ends at the LB IP
# with no reverse-DNS path back to the pod FQDN.
#
# What the migration does:
#   - Replaces every OM pod's keytab with one that carries BOTH the old
#     per-pod principal AND the new stable principal (phase 3).
#   - Switches the OM config to announce the stable principal so clients
#     can compute a literal SPN that does not depend on the LB topology
#     (phase 4).
#   - After every client has been re-pointed at the stable principal,
#     retires the per-pod entries from the pod keytabs (phase 7).
#
# Each phase is a separate sub-command so you can pace the rollout. The
# script does not touch the KDC — those steps are documented in the
# README and must be performed by the KDC operator with kadmin access.
#
# Prerequisites:
#   - Per-pod merged keytabs already produced by the KDC team and dropped
#     into $KEYTAB_DIR with names matching the pod hostnames (e.g.
#     om-0-0.keytab, om-1-0.keytab, om-2-0.keytab).
#   - A slim keytab containing ONLY the stable principal, at
#     $KEYTAB_DIR/om-omservice-only.keytab, for the cleanup phase.
#
# Usage:
#   ./migrate-to-stable-principal.sh preflight
#   ./migrate-to-stable-principal.sh roll
#   ./migrate-to-stable-principal.sh config
#   # → roll out the new principal to all client hosts here …
#   ./migrate-to-stable-principal.sh cleanup
#
#   DRY_RUN=true ./migrate-to-stable-principal.sh roll     # print, don't apply
#
# Tunables (all optional, environment-driven):
#   NAMESPACE          k8s namespace            (default: dev-suslov)
#   REALM              Kerberos realm           (default: VM.ESRT.CLOUD.SBRF.RU)
#   SERVICE_ID         ozone OM service ID      (default: omservice)
#   OM_PODS            space-separated pod list (default: om-0-0 om-1-0 om-2-0)
#   KEYTAB_DIR         where merged keytabs sit (default: /tmp/om-keytab-migration)
#   SECRET_NAME        Secret name              (default: om-keytabs)
#   CONFIGMAP_NAME     ConfigMap name           (default: config)
#   KEYTAB_PATH        path inside the pod      (default: /etc/security/keytabs/om.keytab)
#   DRY_RUN            true → print only        (default: false)

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================
NAMESPACE="${NAMESPACE:-dev-suslov}"
REALM="${REALM:-VM.ESRT.CLOUD.SBRF.RU}"
SERVICE_ID="${SERVICE_ID:-omservice}"
NEW_PRINC="om/${SERVICE_ID}@${REALM}"
read -r -a OM_PODS <<< "${OM_PODS:-om-0-0 om-1-0 om-2-0}"
KEYTAB_DIR="${KEYTAB_DIR:-/tmp/om-keytab-migration}"
SECRET_NAME="${SECRET_NAME:-om-keytabs}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-config}"
KEYTAB_PATH="${KEYTAB_PATH:-/etc/security/keytabs/om.keytab}"
BACKUP_DIR="${BACKUP_DIR:-$KEYTAB_DIR/backup-$(date +%Y%m%d-%H%M%S)}"
DRY_RUN="${DRY_RUN:-false}"
CONFIG_KEY="OZONE-SITE.XML_ozone.om.kerberos.principal"

# ============================================================================
# Helpers
# ============================================================================
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'
BLUE=$'\033[0;34m'; NC=$'\033[0m'

log()  { printf '%s[%s]%s %s\n' "$BLUE" "$(date +%H:%M:%S)" "$NC" "$*"; }
warn() { printf '%s[%s] WARN: %s%s\n' "$YELLOW" "$(date +%H:%M:%S)" "$*" "$NC"; }
err()  { printf '%s[%s] ERROR: %s%s\n' "$RED"    "$(date +%H:%M:%S)" "$*" "$NC"; }
ok()   { printf '%s[%s] OK: %s%s\n'    "$GREEN"  "$(date +%H:%M:%S)" "$*" "$NC"; }

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '%sDRY-RUN: %s%s\n' "$YELLOW" "$*" "$NC"
  else
    eval "$@"
  fi
}

# Read a Y/N answer; "abort" exits immediately.
confirm() {
  local prompt="$1"
  echo
  read -r -p "$prompt [y/N/abort] " ans
  case "${ans:-}" in
    y|Y|yes|YES) return 0 ;;
    a|abort|ABORT) err "operator aborted"; exit 1 ;;
    *) warn "phase skipped by operator"; return 1 ;;
  esac
}

phase_header() {
  echo
  printf '%s================================================================================%s\n' "$BLUE" "$NC"
  printf '%s%s%s\n' "$BLUE" "$1" "$NC"
  printf '%s================================================================================%s\n' "$BLUE" "$NC"
}

# Detect the current OM leader. Empty if not detectable (single-OM or auth issue).
detect_leader() {
  local out=""
  for p in "${OM_PODS[@]}"; do
    out=$(kubectl -n "$NAMESPACE" exec "$p" -- \
      ozone admin om roles -id="$SERVICE_ID" 2>/dev/null \
      | awk -F: '/LEADER/ {print $1}' | head -1) || true
    if [[ -n "$out" ]]; then
      echo "$out"; return
    fi
  done
}

# Roll the listed pods one at a time, with a Kerberos-login sanity check.
# Args: $1 = expected principal in the log line; rest = pod names in order.
roll_pods() {
  local expected="$1"; shift
  for pod in "$@"; do
    log "Restarting $pod ..."
    run "kubectl -n $NAMESPACE delete pod $pod --wait=true"
    log "Waiting for $pod readiness (180s) ..."
    run "kubectl -n $NAMESPACE wait --for=condition=ready pod/$pod --timeout=180s"
    sleep 5
    if [[ "$DRY_RUN" == "true" ]]; then
      warn "DRY-RUN: skipping login verification on $pod"
      continue
    fi
    local line
    line=$(kubectl -n "$NAMESPACE" logs "$pod" 2>&1 \
      | grep -E "Login successful for user $expected" | head -1 || true)
    if [[ -z "$line" ]]; then
      err "$pod did not log expected principal '$expected'. Recent log:"
      kubectl -n "$NAMESPACE" logs "$pod" --tail=40 | grep -E "Login|kerberos|ERROR" | tail -10 || true
      err "Aborting. Re-check the keytab content and the configmap."
      exit 1
    fi
    ok "$pod: ${line#*INFO }"
  done
}

# Order pods with the current leader last so we touch followers first.
ordered_pods() {
  local leader="$1"
  local p
  for p in "${OM_PODS[@]}"; do
    [[ "$p" != "$leader" ]] && printf '%s\n' "$p"
  done
  [[ -n "$leader" ]] && printf '%s\n' "$leader"
}

# ============================================================================
# Phase 0 — Pre-flight
# ============================================================================
phase_0_preflight() {
  phase_header "Phase 0 — Pre-flight"
  log "namespace=$NAMESPACE realm=$REALM service-id=$SERVICE_ID"
  log "new principal=$NEW_PRINC"
  log "pods=${OM_PODS[*]}"
  log "keytab source=$KEYTAB_DIR"
  log "backup dir=$BACKUP_DIR"
  log "dry-run=$DRY_RUN"

  for pod in "${OM_PODS[@]}"; do
    if ! kubectl -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1; then
      err "Pod $pod not found in namespace $NAMESPACE"
      exit 1
    fi
  done
  ok "All ${#OM_PODS[@]} pods present in $NAMESPACE"

  if ! command -v klist >/dev/null 2>&1; then
    warn "klist not on PATH; pre-flight will skip keytab content verification"
  fi

  for pod in "${OM_PODS[@]}"; do
    local kt="$KEYTAB_DIR/${pod}.keytab"
    if [[ ! -f "$kt" ]]; then
      err "Missing merged keytab: $kt"
      err "Expected: keytab containing BOTH the per-pod principal AND $NEW_PRINC"
      err "Have the KDC team produce it via:"
      err "  kadmin -q 'ktadd -norandkey -k $kt om/${pod}.om.${NAMESPACE}.svc.cluster.local@$REALM $NEW_PRINC'"
      exit 1
    fi
    if command -v klist >/dev/null 2>&1 ; then
      if ! klist -k "$kt" 2>/dev/null | grep -q "$NEW_PRINC" ; then
        err "Keytab $kt does not contain $NEW_PRINC"
        err "Content:"; klist -e -k -t "$kt" || true
        exit 1
      fi
      local pod_fqdn="${pod}.om.${NAMESPACE}.svc.cluster.local"
      if ! klist -k "$kt" 2>/dev/null | grep -q "om/$pod_fqdn" ; then
        warn "Keytab $kt does not contain the per-pod principal om/$pod_fqdn@$REALM."
        warn "This is acceptable only if you intend to skip phase-3 verification."
      fi
    fi
    ok "$pod merged keytab present: $kt"
  done

  mkdir -p "$BACKUP_DIR"
  if kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" >/dev/null 2>&1 ; then
    kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" -o yaml > "$BACKUP_DIR/secret-original.yaml"
    ok "Backed up Secret/$SECRET_NAME → $BACKUP_DIR/secret-original.yaml"
  else
    warn "Secret $SECRET_NAME not found; will be created in phase 3"
  fi
  if kubectl -n "$NAMESPACE" get configmap "$CONFIGMAP_NAME" >/dev/null 2>&1 ; then
    kubectl -n "$NAMESPACE" get configmap "$CONFIGMAP_NAME" -o yaml > "$BACKUP_DIR/configmap-original.yaml"
    ok "Backed up ConfigMap/$CONFIGMAP_NAME → $BACKUP_DIR/configmap-original.yaml"
    local current_princ
    current_princ=$(kubectl -n "$NAMESPACE" get configmap "$CONFIGMAP_NAME" \
      -o jsonpath="{.data['$CONFIG_KEY']}" 2>/dev/null || true)
    log "Current $CONFIG_KEY = '${current_princ:-(unset)}'"
  else
    warn "ConfigMap $CONFIGMAP_NAME not found; phase 4 will fail unless you set CONFIGMAP_NAME"
  fi

  for pod in "${OM_PODS[@]}"; do
    if kubectl -n "$NAMESPACE" exec "$pod" -- test -f "$KEYTAB_PATH" >/dev/null 2>&1 ; then
      if kubectl -n "$NAMESPACE" cp "$pod:$KEYTAB_PATH" "$BACKUP_DIR/${pod}.keytab.bak" 2>/dev/null ; then
        ok "Backed up $pod:$KEYTAB_PATH → $BACKUP_DIR/${pod}.keytab.bak"
      else
        warn "Could not copy keytab out of $pod (continuing)"
      fi
    fi
  done

  local leader
  leader=$(detect_leader || true)
  if [[ -n "$leader" ]]; then
    ok "Current OM leader: $leader (followers will be rolled first)"
  else
    warn "Could not detect OM leader; pods will be rolled in configured order"
  fi
}

# ============================================================================
# Phase 3 — Roll merged keytabs onto pods (no config change)
# ============================================================================
phase_3_roll_keytabs() {
  phase_header "Phase 3 — Roll merged keytabs (per-pod + $NEW_PRINC)"
  cat <<EOF
After this phase each OM pod will:
  - have BOTH its old per-pod principal AND $NEW_PRINC in its keytab
  - still announce the OLD principal to clients (config unchanged)
  - be able to decrypt tickets for either principal

If anything goes wrong, restore from $BACKUP_DIR/secret-original.yaml.
EOF
  if ! confirm "Proceed with phase 3?"; then return; fi

  local cmd="kubectl -n $NAMESPACE create secret generic $SECRET_NAME"
  for pod in "${OM_PODS[@]}"; do
    cmd+=" --from-file=${pod}.keytab=$KEYTAB_DIR/${pod}.keytab"
  done
  cmd+=" --dry-run=client -o yaml | kubectl -n $NAMESPACE apply -f -"
  log "Updating Secret/$SECRET_NAME with merged keytabs ..."
  run "$cmd"
  ok "Secret updated"

  local leader; leader=$(detect_leader || true)
  log "Roll order (followers first): $(ordered_pods "$leader" | tr '\n' ' ')"

  # Each pod's expected login line is the OLD per-pod principal — config
  # hasn't changed yet, only the keytab has.
  for pod in $(ordered_pods "$leader") ; do
    local pod_fqdn="${pod}.om.${NAMESPACE}.svc.cluster.local"
    local expected="om/$pod_fqdn@$REALM"
    roll_pods "$expected" "$pod"
  done

  ok "Phase 3 complete — pods carry both keys, still announce the old principal."
  log "Next: run '$0 config' once you're ready to switch the OM config."
}

# ============================================================================
# Phase 4 — Switch OM config to announce om/<service>
# ============================================================================
phase_4_switch_config() {
  phase_header "Phase 4 — Switch OM config: announce $NEW_PRINC"
  cat <<EOF
After this phase each OM pod will:
  - log in as $NEW_PRINC
  - announce $NEW_PRINC during the SASL NEGOTIATE step
  - still accept tickets for the per-pod principal (its key is in the keytab)

Clients dialing the LB VIP with ozone.om.kerberos.principal=$NEW_PRINC
will now succeed without any principal.pattern relaxation.

If anything goes wrong, restore from $BACKUP_DIR/configmap-original.yaml.
EOF
  if ! confirm "Proceed with phase 4?"; then return; fi

  log "Patching $CONFIG_KEY=$NEW_PRINC into ConfigMap/$CONFIGMAP_NAME ..."
  # Use --type=merge with a JSON patch so we only touch one key.
  local patch
  patch=$(printf '{"data":{"%s":"%s"}}' "$CONFIG_KEY" "$NEW_PRINC")
  run "kubectl -n $NAMESPACE patch configmap $CONFIGMAP_NAME --type=merge -p '$patch'"
  ok "ConfigMap patched"

  local leader; leader=$(detect_leader || true)
  log "Roll order (followers first): $(ordered_pods "$leader" | tr '\n' ' ')"

  for pod in $(ordered_pods "$leader") ; do
    roll_pods "$NEW_PRINC" "$pod"
  done

  ok "Phase 4 complete — all pods now announce $NEW_PRINC."
  cat <<EOF

Next steps before phase 7:
  1. From the failing client host, validate:
       kdestroy
       kinit <user>@$REALM
       ozone fs -ls ofs://$SERVICE_ID/
     Expected: no 'Server has invalid Kerberos principal' line, listing returns.
  2. Roll the same ozone.om.kerberos.principal=$NEW_PRINC into every
     client host's ozone-site.xml. Drop any prior ozone.om.kerberos.principal.pattern=*
     work-around at the same time.
  3. Once every client is using the literal principal, run '$0 cleanup'.

EOF
}

# ============================================================================
# Phase 7 — Cleanup: drop per-pod principals from the pod keytabs
# ============================================================================
phase_7_cleanup() {
  phase_header "Phase 7 — Cleanup: shrink keytabs to only $NEW_PRINC"
  warn "PREREQUISITE: every client must already be using ozone.om.kerberos.principal=$NEW_PRINC."
  warn "After this phase, OM keytabs will no longer carry the per-pod principals."
  warn "Any client still expecting the old SPN will break."

  local slim="$KEYTAB_DIR/om-omservice-only.keytab"
  if [[ ! -f "$slim" ]]; then
    err "Missing slim keytab: $slim"
    err "Have the KDC team produce it:"
    err "  kadmin -q 'ktadd -norandkey -k $slim $NEW_PRINC'"
    exit 1
  fi
  if command -v klist >/dev/null 2>&1 ; then
    log "Slim keytab content:"
    klist -e -k -t "$slim" || true
    if klist -k "$slim" 2>/dev/null | grep -q 'om/.*\.svc\.cluster\.local' ; then
      err "Slim keytab still contains pod-FQDN entries; rebuild it with ONLY $NEW_PRINC."
      exit 1
    fi
  fi
  if ! confirm "Have all clients been migrated to $NEW_PRINC? Proceed with cleanup?"; then return; fi

  local cmd="kubectl -n $NAMESPACE create secret generic $SECRET_NAME"
  for pod in "${OM_PODS[@]}"; do
    cmd+=" --from-file=${pod}.keytab=$slim"
  done
  cmd+=" --dry-run=client -o yaml | kubectl -n $NAMESPACE apply -f -"
  log "Updating Secret/$SECRET_NAME with the slim keytab on every pod ..."
  run "$cmd"

  local leader; leader=$(detect_leader || true)
  for pod in $(ordered_pods "$leader") ; do
    roll_pods "$NEW_PRINC" "$pod"
  done

  ok "Phase 7 complete — every OM pod now holds only $NEW_PRINC."
  cat <<EOF

Final KDC-side step (run on the KDC host):
  kadmin -p admin/admin
EOF
  for pod in "${OM_PODS[@]}"; do
    local fqdn="${pod}.om.${NAMESPACE}.svc.cluster.local"
    printf '  > delprinc om/%s@%s\n' "$fqdn" "$REALM"
  done
  echo
}

# ============================================================================
# Entry point
# ============================================================================
usage() {
  cat <<EOF
Usage: $0 [preflight|roll|config|cleanup|all]

Phases:
  preflight  pre-flight sanity checks + backups, no changes
  roll       push merged keytabs to OM pods + rolling restart  (phase 3)
  config     switch OM config to announce $NEW_PRINC + rolling restart  (phase 4)
  cleanup    drop per-pod principals from pod keytabs           (phase 7, irreversible)
  all        runs preflight + roll + config; stops before cleanup

Environment variables (current values shown):
  NAMESPACE=$NAMESPACE
  REALM=$REALM
  SERVICE_ID=$SERVICE_ID
  OM_PODS='${OM_PODS[*]}'
  KEYTAB_DIR=$KEYTAB_DIR
  SECRET_NAME=$SECRET_NAME
  CONFIGMAP_NAME=$CONFIGMAP_NAME
  KEYTAB_PATH=$KEYTAB_PATH
  DRY_RUN=$DRY_RUN

Drop these files in \$KEYTAB_DIR before running:
  om-0-0.keytab, om-1-0.keytab, om-2-0.keytab     (merged: per-pod + $NEW_PRINC)
  om-omservice-only.keytab                        (cleanup phase only)
EOF
}

main() {
  case "${1:-}" in
    preflight|0)       phase_0_preflight ;;
    roll|3)            phase_0_preflight; phase_3_roll_keytabs ;;
    config|4)          phase_0_preflight; phase_4_switch_config ;;
    cleanup|7)         phase_0_preflight; phase_7_cleanup ;;
    all)
      phase_0_preflight
      phase_3_roll_keytabs
      phase_4_switch_config
      warn "Stopping before phase 7 — that step is irreversible and must wait"
      warn "until every client has switched to ozone.om.kerberos.principal=$NEW_PRINC."
      warn "Re-run '$0 cleanup' when ready."
      ;;
    -h|--help|help|"") usage; exit 0 ;;
    *) err "unknown phase: $1"; echo; usage; exit 1 ;;
  esac
}

main "$@"
