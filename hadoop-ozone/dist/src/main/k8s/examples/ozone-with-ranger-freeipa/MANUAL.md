# Manual walk-through

This walks you through a from-scratch run on your machine and a hand-driven
test using the `cli` pod. The automated `acl-test` Job does all of these
checks unattended — this doc is for when you want to poke at the cluster.

## 0. Prerequisites

Tools on the host: `docker`, `kubectl`, `minikube`, `mvn`. The whole thing
runs in a single minikube node.

## 1. Resource sizing

Measured during the e2e run (1 OM + 1 SCM + 1 DN + 1 S3G + Ranger Admin +
Postgres + OpenLDAP + minikube control plane):

| Resource | At rest (after acl-test passes) | Used during image loads / build |
| -------- | -------------------------------: | ------------------------------: |
| Memory   | 5.8 GiB / 12 GiB allocated       | up to 11 GiB during peak        |
| CPU      | ~40 % of 6 cores                 | up to 100 % during mvn package  |

**Use at least 12 GiB / 6 cores.** With 8 GiB the API server falls over
under load (TLS handshake timeouts) — that was the single biggest source of
churn during development.

```
minikube start \
  --profile ozone-ranger-ldap \
  --memory 12288 \
  --cpus 6 \
  --kubernetes-version v1.30.0 \
  --driver=docker
```

## 2. Build images

Two host-side images are required: the Ozone build (with the
`ranger-ozone-plugin` + `ranger-ipa-userstore` JARs baked in and the OM
classpath patched) and `openldap-seeded` (LDIFs baked in so osixia's
chown-on-startup doesn't trip on a read-only configmap mount).

```bash
cd /work/sber/component-ranger-plugins
mvn -pl :ranger-distro,:ranger-ozone-plugin,:ranger-ozone-plugin-shim,:ranger-ipa-userstore,\
:ranger-plugin-classloader,:ranger-plugins-common,:ranger-plugins-audit,:ranger-plugins-cred \
  -am -P sdp-build-ranger-ozone-plugin package -DskipTests -q

cd /work/sber/component-ozone
mvn -pl :ozone -am package -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -q

cd hadoop-ozone/dist/src/main/k8s/examples/ozone-with-ranger-freeipa
cp /work/sber/component-ranger-plugins/target/ranger-2.4.0-ozone-plugin.tar.gz /work/sber/component-ozone/
cp Dockerfile.ozone /work/sber/component-ozone/Dockerfile.ozone
docker build -t apache/ozone-test:local -f /work/sber/component-ozone/Dockerfile.ozone /work/sber/component-ozone/
rm /work/sber/component-ozone/Dockerfile.ozone /work/sber/component-ozone/ranger-2.4.0-ozone-plugin.tar.gz

docker build -t openldap-seeded:local -f Dockerfile.openldap .
```

## 3. Load images into minikube + pull the rest

```bash
minikube --profile ozone-ranger-ldap image load apache/ozone-test:local
minikube --profile ozone-ranger-ldap image load openldap-seeded:local

# apache/ranger 2.4.0 only ships arm64 on Docker Hub — 2.8.0 is multi-arch
# and is API-compatible with our 2.4.0 plugin protocol.
for img in apache/ranger:2.8.0 postgres:12 rancher/kubectl:v1.30.7 \
           curlimages/curl:8.10.1 busybox:1.36 python:3.11-alpine; do
  minikube --profile ozone-ranger-ldap image pull "$img"
done
```

## 4. Apply

Use `deploy.sh` instead of `kubectl apply -k .` — the daemon pods need
the `keytabs` Secret at startup, and the secret can't exist until the
KDC has been seeded with the test principals. `deploy.sh` orchestrates
that ordering inline.

```bash
./deploy.sh
```

Expected sequence (~3 min total):

| Pod                              | Becomes Ready after |
| -------------------------------- | -------------------: |
| `kdc`                            | ~5 s |
| `ranger-db`                      | ~10 s |
| `openldap`                       | ~30 s |
| `scm-0`, `s3g-0`                 | ~45 s |
| `datanode-0`,`-1`,`-2`           | ~60 s (3 replicas for RATIS/THREE quorum) |
| `om-0`                           | ~90 s (wait-scm + init) |
| `ranger-admin`                   | ~120 s (DB schema bootstrap) |
| `ranger-policies-bootstrap` Job  | Complete ~150 s |
| `acl-bootstrap` Job              | Complete ~180 s |
| `acl-test` Job                   | Complete ~210 s |
| `acl-test-s3` Job                | Complete ~220 s |
| `cli` pod                        | Running ~50 s |

If the Ranger Admin pod CrashLoops with "DB schema setup failed", just wait
— the wait-db init pauses it until Postgres is ready, then the embedded
setup script runs once and succeeds.

### Auth model — split-Kerberos (external) + SIMPLE (interservice)

OM client RPC and the s3g HTTP listener use Kerberos. OM↔SCM and DN↔SCM
inter-service hops stay SIMPLE via Step M/N SIMPLE-sibling RPC ports
(`ozone.scm.service.rpc-address=0.0.0.0:9866`,
`ozone.om.service.rpc-address=0.0.0.0:9864`). Mirrors the apache
reference example `ozone-split-kerberos-external-kdc`.

Principals are stable + short (no `_HOST`, no pod FQDN):
- `om/om@EXAMPLE.COM`, `scm/scm@EXAMPLE.COM`, `dn/dn@EXAMPLE.COM`,
  `s3g/s3g@EXAMPLE.COM` — pre-baked in `apache/ozone-testkrb5`.
- `testuser@EXAMPLE.COM`, `testuser2@EXAMPLE.COM` — added by `deploy.sh`
  before keytabs are minted into Secrets.

HTTP SPNEGO is off — UIs and the s3g secret endpoint stay simple.

The Ranger plugin REST poll runs as `secureMode=false` via
`ranger.plugin.ozone.forceNonKerberos=true` because the bundled
ranger-admin uses HTTP basic auth, not SPNEGO.

`ozone.s3g.volume.name=vol1` maps the S3 bucket namespace onto vol1, so
the same Ranger policies that guard vol1/bucket{1,2,3} apply to S3
requests through s3g.

## 5. Watch the automated tests

```bash
kubectl -n ozone-test logs -f job/acl-test       # OM RPC scenarios
kubectl -n ozone-test logs -f job/acl-test-s3    # s3g scenarios (same matrix)
```

Expected last lines: `RESULT: all ACL assertions passed` and
`RESULT: all S3 ACL assertions passed`.

## 6. Hand-driven test from the cli pod

The `cli` pod is plain `apache/ozone-test:local`, no LDAP credentials
mounted — group resolution is happening inside OM via `IpaGroupMapping`.
With Kerberos turned on you have to `kinit` first; the client then talks
to OM as the kinit'd principal.

```bash
kubectl -n ozone-test exec -it cli -- bash
```

Inside the pod:

### 6.1 Sanity check — Ozone topology

```bash
kinit -kt /testuser-keytabs/testuser2.keytab testuser2@EXAMPLE.COM
ozone sh volume list /                       # → OK; testuser2 is admin
```

### 6.2 SDPOZN-2434 headline scenario

The whole point of the IPA-sync plugin: `testuser` belongs to `group1` in
LDAP, and Ranger's bucket2 policy is granted to `group1`. If group resolution
works, the read succeeds. If it didn't, the read would fail with
PERMISSION_DENIED — same as bucket3 (which is granted to `group2`,
where `testuser` does NOT belong).

```bash
echo hello > /tmp/payload

# --- testuser scenarios ---
kdestroy -A
kinit -kt /testuser-keytabs/testuser.keytab testuser@EXAMPLE.COM

# direct user policy on vol1/bucket1
ozone sh key put o3://om/vol1/bucket1/manual.txt /tmp/payload   # → OK
ozone sh key get o3://om/vol1/bucket1/manual.txt /tmp/got       # → OK
ozone sh key list o3://om/vol1/bucket1                          # → OK

# group1 policy on vol1/bucket2 (read-only)
ozone sh key get o3://om/vol1/bucket2/seed /tmp/got             # → OK — proves group1 membership resolved
ozone sh key put o3://om/vol1/bucket2/manual.txt /tmp/payload   # → PERMISSION_DENIED (group1 has read only)

# no group2 → no access to bucket3
ozone sh key get o3://om/vol1/bucket3/seed /tmp/got             # → PERMISSION_DENIED
ozone sh key put o3://om/vol1/bucket3/manual.txt /tmp/payload   # → PERMISSION_DENIED

# --- testuser2 scenarios ---
kdestroy -A
kinit -kt /testuser-keytabs/testuser2.keytab testuser2@EXAMPLE.COM

ozone sh volume create vol-manual                               # → OK (default policy grants testuser2)
ozone sh bucket create vol-manual/box                           # → OK
ozone sh key put o3://om/vol-manual/box/manual.txt /tmp/payload # → OK
ozone sh key get o3://om/vol-manual/box/manual.txt /tmp/got     # → OK
ozone sh bucket delete vol-manual/box
ozone sh volume delete vol-manual
```

### 6.2.1 Same matrix via s3g

`acl-test-s3` exercises the same scenarios end-to-end through s3g. OM
mints a SigV4 access/secret pair bound to your Kerberos identity, and
`ozone.s3g.volume.name=vol1` maps the S3 namespace onto vol1.

```bash
kdestroy -A
kinit -kt /testuser-keytabs/testuser.keytab testuser@EXAMPLE.COM
ozone s3 revokesecret -y >/dev/null 2>&1 || true
eval $(ozone s3 getsecret | awk -F= '/^awsAccessKey/  {print "export AWS_ACCESS_KEY_ID="$2}
                                     /^awsSecret/     {print "export AWS_SECRET_ACCESS_KEY="$2}')
export AWS_REGION=us-east-1

aws --endpoint http://s3g:9878 s3 cp /tmp/payload s3://bucket1/k1     # → OK
aws --endpoint http://s3g:9878 s3 cp s3://bucket2/seed /tmp/got       # → OK
aws --endpoint http://s3g:9878 s3 cp /tmp/payload s3://bucket2/k1     # → AccessDenied
```


### 6.3 Verify the LDAP-side state (the source of truth)

From the cli pod:

```bash
apk add openldap-clients   # cli is alpine-y; on rocky/centos: yum install openldap-clients
ldapsearch -x -H ldap://ldap.ozone-test.svc.cluster.local \
  -D 'cn=admin,dc=example,dc=test' -w AdminPass1! \
  -b 'dc=example,dc=test' '(|(uid=testuser*)(cn=group*))' uid cn member
```

You should see `cn=group1` with `member: uid=testuser,…` and
`cn=group2` with `member: uid=testuser2,…`.

### 6.4 Watch the plugin do the sync

```bash
kubectl -n ozone-test logs om-0 | grep -i IpaSyncJob
```

Expected lines (one per refresh interval, default 10 s in this profile):

```
IpaSyncJob started; interval=10000ms url=ldap://ldap.ozone-test.svc.cluster.local:389
IpaSyncJob: refreshed 2 users
IpaSyncJob: refreshed 2 users
```

### 6.5 Live-test the IPA sync (the actually-novel bit)

Add a new LDAP user/group and watch testuser pick up the new group inside
OM without restarting anything.

In one shell, tail the OM logs:

```bash
kubectl -n ozone-test logs -f om-0 | grep -i IpaSync
```

In another, edit the LDAP tree:

```bash
kubectl -n ozone-test exec -it deploy/openldap -- bash -c "ldapadd -x -H ldap://localhost \
  -D 'cn=admin,dc=example,dc=test' -w AdminPass1! <<'EOF'
dn: cn=group3,ou=groups,ou=accounts,dc=example,dc=test
objectClass: groupOfNames
cn: group3
member: uid=testuser,ou=users,ou=accounts,dc=example,dc=test
EOF"
```

Within 10 seconds you'll see `IpaSyncJob: refreshed 2 users` in the OM
log — the cache now has testuser → {group1, group3}. Add a Ranger policy
referencing `group3` and that policy will apply to `testuser` on its next
authorization check.

## 6.6 Open the Ranger Admin UI in a browser

The `ranger-admin` Service is ClusterIP (in-cluster only). Two ways to reach
it from your host browser:

**Port-forward (simplest, no Service change):**

```bash
kubectl -n ozone-test port-forward svc/ranger-admin 6080:6080
```

Then browse to <http://localhost:6080> and log in as **`admin` / `RangerAdmin1!`**.
Stop the forward with `Ctrl-C` when done.

What to look at:
- **Access Manager → Resource Based Policies → `ozonedev`** — you'll see the
  five policies the bootstrap created (`p-vol1-traverse`, `p-testuser-bucket1`,
  `p-group1-bucket2-r`, `p-group2-bucket3-rw`, plus the extended default
  `all - volume, bucket, key` that grants `testuser2` and `om`).
- **Settings → Users/Groups/Roles** — `testuser`, `testuser2`, `om`, `group1`,
  `group2` were pre-created by the policies bootstrap.
- **Audits → Access** is empty (we set `xasecure.audit.is.enabled=false` for
  the test cluster). Re-enable it in `config-configmap.yaml` if you want to
  see per-request audit rows from OM.

**minikube tunnel (one URL for everything ClusterIP):**

```bash
minikube --profile ozone-ranger-ldap tunnel        # leave running in another terminal
minikube --profile ozone-ranger-ldap service ranger-admin -n ozone-test --url
```

The second command prints something like `http://127.0.0.1:NNNNN`; open it.

## 6.7 See user→group mapping

Three layers — pick the one that fits the question.

| Layer | What you see | How |
| --- | --- | --- |
| **LDAP source of truth** | raw `member:` attrs on each group | `ldapsearch` against the openldap pod |
| **OM Ranger plugin view** | cache-refresh heartbeat | `kubectl logs om-0` grep |
| **Browser UI** | click-through tree view | LAM (LDAP Account Manager) — optional add-on |

### CLI — LDAP source of truth

```bash
kubectl -n ozone-test exec deploy/openldap -- ldapsearch -x -H ldap://localhost \
  -D 'cn=admin,dc=example,dc=test' -w AdminPass1! \
  -b 'ou=groups,ou=accounts,dc=example,dc=test' \
  '(objectClass=groupOfNames)' cn member
```

Expected:

```
dn: cn=group1,ou=groups,ou=accounts,dc=example,dc=test
cn: group1
member: uid=testuser,ou=users,ou=accounts,dc=example,dc=test

dn: cn=group2,ou=groups,ou=accounts,dc=example,dc=test
cn: group2
member: uid=testuser2,ou=users,ou=accounts,dc=example,dc=test
```

### OM Ranger plugin view (the cache that drives authorization)

```bash
kubectl -n ozone-test logs om-0 | grep IpaSyncJob
```

Expected (one line per refresh, default 10 s in this profile):

```
IpaSyncJob started; interval=10000ms url=ldap://ldap.ozone-test.svc.cluster.local:389
IpaSyncJob: refreshed 2 users
IpaSyncJob: refreshed 2 users
```

`refreshed N users` is the size of the in-process `user → groups` map.
This is what Ranger reads on every authorization call.

### Browser UI — LAM (optional)

Not in `kustomization.yaml`. Apply on demand:

```bash
kubectl apply -f phpldapadmin.yaml             # filename kept; image is LAM
kubectl -n ozone-test port-forward svc/ldap-ui 8888:80
```

(phpLDAPadmin was the first attempt but its osixia image has a PHP-FPM
permission bug that returns 403 on every page — LAM works out of the box,
same Service name shape.)

Open <http://localhost:8888/>. First-time setup is one-shot:

1. Top right: **LAM configuration → Edit server profiles**
2. Master password: **`lam`**
3. On the **lam** profile, set:
   - **Server address**: `ldap://ldap.ozone-test.svc.cluster.local:389`
   - **Tree suffix**: `dc=example,dc=test`
   - **Valid users (list of admins)**: `cn=admin,dc=example,dc=test`
4. **Save**, then go back to the login page.
5. Profile = **lam**, Password = **`AdminPass1!`** (openldap admin)

Click **Tree view**. Expand `dc=example,dc=test → ou=accounts → ou=groups
→ cn=group1`. The `member:` attribute lists `uid=testuser,...`. Toggle to
`cn=group2` for testuser2.

## 7. Tear down

```bash
kubectl delete -k .
kubectl -n ozone-test delete deployment kdc service kdc configmap krb5 --ignore-not-found
kubectl -n ozone-test delete secret keytabs testuser-keytabs --ignore-not-found
minikube --profile ozone-ranger-ldap delete
```
