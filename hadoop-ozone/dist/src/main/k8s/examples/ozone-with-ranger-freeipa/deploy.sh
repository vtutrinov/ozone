#!/usr/bin/env bash
# Deploy the ozone-with-ranger-freeipa stack with split-Kerberos enabled
# (external = KERBEROS, internal = SIMPLE). The stack uses short, stable
# principals (om/om, scm/scm, dn/dn, s3g/s3g) that apache/ozone-testkrb5
# already ships pre-baked — none of them are bound to a k8s pod FQDN, so
# the same configmap shape works for every pod in every namespace.
#
# Why a script (vs. pure `kubectl apply -k .`):
#   The daemon pods need a `keytabs` Secret at startup. The secret can't
#   exist until the KDC has been seeded with the testuser principals and
#   the keytabs extracted. Pure kustomize can't express that ordering
#   without rolling restarts, so we run KDC + keytab bootstrap inline and
#   only then apply the rest of the stack.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="ozone-test"
REALM="EXAMPLE.COM"

log() { printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }

log "1/6 namespace + krb5 configmap + KDC"
kubectl apply -f "$DIR/namespace.yaml"
# kdc.yaml and krb5-configmap.yaml don't set metadata.namespace (kustomize
# normally injects it). Apply them explicitly into $NS so deploy.sh's
# rollout/exec calls find them.
kubectl -n "$NS" apply -f "$DIR/krb5-configmap.yaml"
kubectl -n "$NS" apply -f "$DIR/kdc.yaml"

log "2/6 wait for KDC pod ready"
kubectl -n "$NS" rollout status deployment/kdc --timeout=120s
KDC_POD=$(kubectl -n "$NS" get pod -l component=kdc -o jsonpath='{.items[0].metadata.name}')
log "KDC pod: $KDC_POD"

log "3/6 add testuser + testuser2 principals (om/scm/dn/s3g are pre-baked)"
kubectl -n "$NS" exec "$KDC_POD" -- bash -c "
  set -e
  for u in testuser testuser2 ; do
    kadmin.local -q 'listprincs' | grep -q \"^\${u}@${REALM}\$\" \
      || kadmin.local -q \"addprinc -randkey \${u}@${REALM}\"
  done
  echo '--- principals used by this test:'
  kadmin.local -q 'listprincs' | grep -E '^(testuser@|testuser2@|om/om@|scm/scm@|dn/dn@|s3g/s3g@)' | sort
"

log "4/6 export keytabs from KDC (ktadd -norandkey preserves the pre-baked keys)"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
kubectl -n "$NS" exec "$KDC_POD" -- bash -c "
  set -e
  rm -f /tmp/*.keytab
  for p in testuser@${REALM} testuser2@${REALM} \
           om/om@${REALM} scm/scm@${REALM} dn/dn@${REALM} s3g/s3g@${REALM} ; do
    kname=\$(echo \$p | tr '/' '_' | cut -d@ -f1)
    kadmin.local -q \"ktadd -norandkey -k /tmp/\${kname}.keytab \$p\"
  done
  ls -la /tmp/*.keytab
"
for kt in testuser testuser2 om_om scm_scm dn_dn s3g_s3g; do
  kubectl -n "$NS" cp "$KDC_POD:/tmp/$kt.keytab" "$WORK/$kt.keytab"
done

log "5/6 create Secrets"
kubectl -n "$NS" create secret generic keytabs \
  --from-file=om.keytab="$WORK/om_om.keytab"   \
  --from-file=scm.keytab="$WORK/scm_scm.keytab" \
  --from-file=dn.keytab="$WORK/dn_dn.keytab"   \
  --from-file=s3g.keytab="$WORK/s3g_s3g.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create secret generic testuser-keytabs \
  --from-file=testuser.keytab="$WORK/testuser.keytab"   \
  --from-file=testuser2.keytab="$WORK/testuser2.keytab" \
  --dry-run=client -o yaml | kubectl apply -f -

log "6/6 apply the rest of the stack"
kubectl apply -k "$DIR"

echo
echo "=== deploy complete ==="
echo "Watch progress with: kubectl -n $NS get pods,jobs -w"
