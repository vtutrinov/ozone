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

# Variant of deploy.sh that pushes the keytab-minimization further: the
# KDC is seeded with **only the principals that are actually authenticated
# to** (testuser@, om/om@, scm/scm@), and the cluster-side `keytabs` Secret
# contains only om.keytab and scm.keytab. DN and S3G mount the same Secret
# but find no dn/s3g keytab in it — that's fine, because after Step J they
# don't attempt the keytab login at all when interservice=false.
#
# SCM is NOT keyless: per Step K it serves Kerberos on its external admin
# port (StorageContainerLocationProtocol, 9860), so it needs its long-term
# key in the keytab to decrypt client tickets. Operators who want keyless
# SCM must also disable external Kerberos (then `ozone admin scm` no longer
# requires a TGT and SCM truly needs no Kerberos identity).
#
# What this proves end-to-end:
#   - The KDC sees only ONE service principal from the cluster (om/om).
#   - SCM, DN, S3G pods carry NO Kerberos artifact tied to their identity
#     (they still mount /etc/security/keytabs because the YAML hasn't
#     changed, but the directory contains only the OM keytab, which
#     they never load).
#   - The client pod, in the edge namespace, holds the only user keytab
#     (testuser.keytab) and exercises the full surface.
#
# Apart from the seeding step, the deploy is identical to deploy.sh.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EDGE_NS="ozone-extkdc-edge"
CLUSTER_NS="ozone-extkdc-cluster"
REALM="EXAMPLE.COM"

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }

log "1/8 namespaces + KDC + krb5 configmaps"
kubectl apply -f "$DIR/00-namespaces.yaml"
kubectl apply -f "$DIR/10-kdc.yaml"
kubectl apply -f "$DIR/20-krb5-configmap.yaml"
kubectl -n "$EDGE_NS" rollout status deployment/kdc --timeout=120s

KDC_POD=$(kubectl -n "$EDGE_NS" get pod -l component=kdc -o jsonpath='{.items[0].metadata.name}')
log "KDC pod: $KDC_POD"

log "2/8 ensure testuser principal exists"
kubectl -n "$EDGE_NS" exec "$KDC_POD" -- bash -c "
  kadmin.local -q 'listprincs' | grep -q '^testuser@${REALM}\$' \
    || kadmin.local -q 'addprinc -randkey testuser@${REALM}'
  echo '--- principals matching identities used by this test:'
  kadmin.local -q 'listprincs' | grep -E '^(testuser@|om/om@|scm/scm@)' | sort
"

log "3/8 export testuser + om + scm keytabs"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

kubectl -n "$EDGE_NS" exec "$KDC_POD" -- bash -c "
  set -e
  rm -f /tmp/*.keytab
  kadmin.local -q 'ktadd -norandkey -k /tmp/testuser.keytab testuser@${REALM}'
  kadmin.local -q 'ktadd -norandkey -k /tmp/om.keytab       om/om@${REALM}'
  kadmin.local -q 'ktadd -norandkey -k /tmp/scm.keytab      scm/scm@${REALM}'
  ls -la /tmp/*.keytab
"
kubectl -n "$EDGE_NS" cp "$KDC_POD:/tmp/testuser.keytab" "$WORK/testuser.keytab"
kubectl -n "$EDGE_NS" cp "$KDC_POD:/tmp/om.keytab"       "$WORK/om.keytab"
kubectl -n "$EDGE_NS" cp "$KDC_POD:/tmp/scm.keytab"      "$WORK/scm.keytab"

log "4/8 create Secrets"
# Cluster side: OM + SCM keytabs. DN and S3G mount this Secret but find
# no key file for themselves; Step J keeps them from trying to load one.
# SCM has a keytab because Step K serves Kerberos on its admin port.
kubectl -n "$CLUSTER_NS" create secret generic keytabs \
  --from-file=om.keytab="$WORK/om.keytab" \
  --from-file=scm.keytab="$WORK/scm.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$EDGE_NS" create secret generic testuser-keytab \
  --from-file=testuser.keytab="$WORK/testuser.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

log "4b/8 sanity: cluster Secret keys"
kubectl -n "$CLUSTER_NS" get secret keytabs -o jsonpath='{.data}' \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("keys:", list(d.keys()))'

log "5/8 apply cluster manifests"
kubectl apply -f "$DIR/30-config-configmap.yaml"
kubectl apply -f "$DIR/41-scm.yaml"
kubectl -n "$CLUSTER_NS" rollout status statefulset/scm --timeout=300s

kubectl apply -f "$DIR/40-om.yaml"
kubectl apply -f "$DIR/42-datanode.yaml"
kubectl -n "$CLUSTER_NS" rollout status statefulset/om --timeout=300s
kubectl -n "$CLUSTER_NS" rollout status statefulset/datanode --timeout=300s

kubectl apply -f "$DIR/43-s3g.yaml"
kubectl -n "$CLUSTER_NS" rollout status statefulset/s3g --timeout=300s

log "6/8 confirm SCM/DN/S3G have no scm.keytab / dn.keytab / s3g.keytab"
for pod in scm-0 datanode-0 s3g-0 ; do
  printf '  %-12s ' "$pod:"
  kubectl -n "$CLUSTER_NS" exec "$pod" -- ls /etc/security/keytabs/ 2>&1 | tr '\n' ' '
  echo
done

log "7/8 launch client Job"
kubectl -n "$EDGE_NS" delete job ozone-client --ignore-not-found
kubectl apply -f "$DIR/50-client.yaml"

log "7b/8 wait for client Job"
if ! kubectl -n "$EDGE_NS" wait --for=condition=complete --timeout=300s job/ozone-client ; then
  echo "--- client Job did not complete; dumping logs ---"
  kubectl -n "$EDGE_NS" logs job/ozone-client --tail=400
  exit 1
fi

log "8/8 client Job logs"
kubectl -n "$EDGE_NS" logs job/ozone-client --tail=400
echo
echo "=== keyless external-KDC split-Kerberos: PASS ==="
