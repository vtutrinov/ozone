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

# Deploy and verify the external-KDC variant of the split-Kerberos example.
#
# Layout (see 00-namespaces.yaml):
#   ozone-extkdc-edge     — KDC + ozone-client Job
#   ozone-extkdc-cluster  — OM, SCM, DN, S3G
#
# Principals used (all short, stable, deliberately NOT pod-FQDN bound):
#   testuser@EXAMPLE.COM
#   om/om@EXAMPLE.COM
#   scm/scm@EXAMPLE.COM
#   dn/dn@EXAMPLE.COM
#   s3g/s3g@EXAMPLE.COM
#
# The first four come pre-baked in apache/ozone-testkrb5 — that's exactly
# the kind of corporate-KDC-friendly entry the deployment mode is designed
# around: short names, no FQDN suffix tied to the k8s cluster topology.
# Only testuser@EXAMPLE.COM has to be added (the image's testuser entries
# are testuser/<daemon-host>, which is service-principal shaped).

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EDGE_NS="ozone-extkdc-edge"
CLUSTER_NS="ozone-extkdc-cluster"
REALM="EXAMPLE.COM"
RANGER_FQDN="ranger-admin.${CLUSTER_NS}.svc.cluster.local"

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }

log "1/8 namespaces + KDC + krb5 configmaps"
kubectl apply -f "$DIR/00-namespaces.yaml"
kubectl apply -f "$DIR/10-kdc.yaml"
kubectl apply -f "$DIR/20-krb5-configmap.yaml"

log "1b/8 wait for KDC pod ready"
kubectl -n "$EDGE_NS" rollout status deployment/kdc --timeout=120s

KDC_POD=$(kubectl -n "$EDGE_NS" get pod -l component=kdc -o jsonpath='{.items[0].metadata.name}')
log "KDC pod: $KDC_POD"

log "2/8 add testuser + ranger-admin SPNEGO principals"
# om/om scm/scm dn/dn s3g/s3g are pre-baked in apache/ozone-testkrb5.
# testuser is the client identity used by 50-client.yaml and the Step P
# tgt-renewal-test (63-tgt-renewal-test.yaml).
# HTTP/<ranger-fqdn>, rangeradmin/<ranger-fqdn>, rangerlookup/<ranger-fqdn>
# are what RangerKRBAuthenticationFilter and the Tomcat connector use to
# accept SPNEGO from OM's Ranger plugin.
kubectl -n "$EDGE_NS" exec "$KDC_POD" -- bash -c "
  set -e
  kadmin.local -q 'listprincs' | grep -q '^testuser@${REALM}$' \
    || kadmin.local -q 'addprinc -randkey testuser@${REALM}'
  for p in HTTP/${RANGER_FQDN} rangeradmin/${RANGER_FQDN} rangerlookup/${RANGER_FQDN} ; do
    kadmin.local -q 'listprincs' | grep -q \"^\${p}@${REALM}\$\" \
      || kadmin.local -q \"addprinc -randkey \${p}@${REALM}\"
  done
  echo '--- principals matching the daemon identities used by this test:'
  kadmin.local -q 'listprincs' | grep -E \"^(testuser@|om/om@|scm/scm@|dn/dn@|s3g/s3g@|HTTP/${RANGER_FQDN}@|rangeradmin/${RANGER_FQDN}@|rangerlookup/${RANGER_FQDN}@)\" | sort
"

log "3/8 export keytabs from KDC pod"
WORK=$(mktemp -d)
trap 'rm -rf \"$WORK\"' EXIT

# ktadd rotates the key on each call, which would invalidate keytabs for any
# of these principals that have already been issued to running daemons. Use
# ktadd -norandkey to preserve the existing key (works for the pre-baked
# entries; testuser was just added with -randkey so its key already exists).
kubectl -n "$EDGE_NS" exec "$KDC_POD" -- bash -c "
  set -e
  rm -f /tmp/*.keytab
  for p in testuser@${REALM} om/om@${REALM} scm/scm@${REALM} dn/dn@${REALM} s3g/s3g@${REALM} \
           HTTP/${RANGER_FQDN}@${REALM} rangeradmin/${RANGER_FQDN}@${REALM} rangerlookup/${RANGER_FQDN}@${REALM} ; do
    kname=\$(echo \$p | tr '/' '_' | cut -d@ -f1)
    kadmin.local -q \"ktadd -norandkey -k /tmp/\${kname}.keytab \$p\"
  done
  ls -la /tmp/*.keytab
"
for kt in testuser om_om scm_scm dn_dn s3g_s3g \
          HTTP_${RANGER_FQDN} rangeradmin_${RANGER_FQDN} rangerlookup_${RANGER_FQDN} ; do
  kubectl -n "$EDGE_NS" cp "$KDC_POD:/tmp/$kt.keytab" "$WORK/$kt.keytab"
done

log "4/8 create Secrets"
kubectl -n "$CLUSTER_NS" create secret generic keytabs \
  --from-file=om.keytab="$WORK/om_om.keytab" \
  --from-file=s3g.keytab="$WORK/s3g_s3g.keytab" \
  --from-file=scm.keytab="$WORK/scm_scm.keytab" \
  --from-file=dn.keytab="$WORK/dn_dn.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$EDGE_NS" create secret generic testuser-keytab \
  --from-file=testuser.keytab="$WORK/testuser.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

# Ranger-admin (the SPNEGO acceptor) consumes its own Secret with the three
# HTTP/rangeradmin/rangerlookup principals.
kubectl -n "$CLUSTER_NS" create secret generic ranger-admin-keytabs \
  --from-file=spnego.keytab="$WORK/HTTP_${RANGER_FQDN}.keytab" \
  --from-file=rangeradmin.keytab="$WORK/rangeradmin_${RANGER_FQDN}.keytab" \
  --from-file=rangerlookup.keytab="$WORK/rangerlookup_${RANGER_FQDN}.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

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

# Step P regression chain — ranger-db, ranger-admin, the policy bootstrap
# Job, and the tgt-renewal-test Job. Skipping these would not break the
# rest of the example; they only validate that OM's Ranger plugin can
# fetch policies via SPNEGO across a TGT lifetime cycle.
log "5b/8 apply Step P regression chain (ranger-db, ranger-admin, bootstrap, test)"
kubectl apply -f "$DIR/60-ranger-db.yaml"
kubectl apply -f "$DIR/61-ranger-admin.yaml"
kubectl apply -f "$DIR/62-ranger-bootstrap.yaml"
kubectl apply -f "$DIR/64-ranger-ozone-security-configmap.yaml"
kubectl -n "$CLUSTER_NS" rollout status deployment/ranger-admin --timeout=600s
kubectl -n "$CLUSTER_NS" wait --for=condition=complete --timeout=15m job/ranger-policies-bootstrap

log "6/8 launch client Job"
kubectl -n "$EDGE_NS" delete job ozone-client --ignore-not-found
kubectl apply -f "$DIR/50-client.yaml"

log "7/8 wait for client Job"
if ! kubectl -n "$EDGE_NS" wait --for=condition=complete --timeout=600s job/ozone-client ; then
  echo "--- client Job did not complete; dumping logs ---"
  kubectl -n "$EDGE_NS" logs job/ozone-client --tail=400
  exit 1
fi

log "8/8 client Job logs"
kubectl -n "$EDGE_NS" logs job/ozone-client --tail=400

log "9/8 launch Step P regression test (tgt-renewal-test)"
kubectl -n "$EDGE_NS" delete job tgt-renewal-test --ignore-not-found
kubectl apply -f "$DIR/63-tgt-renewal-test.yaml"

if ! kubectl -n "$EDGE_NS" wait --for=condition=complete --timeout=600s job/tgt-renewal-test ; then
  echo "--- tgt-renewal-test did not complete; dumping logs ---"
  kubectl -n "$EDGE_NS" logs job/tgt-renewal-test --tail=400
  exit 1
fi
kubectl -n "$EDGE_NS" logs job/tgt-renewal-test --tail=400

echo
echo "=== external-KDC split-Kerberos + Step P regression: PASS ==="
