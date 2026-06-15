# Split-Kerberos with an external KDC

This example exercises the split-Kerberos mode in the configuration that
motivated the feature: a corporate KDC that lives **outside** the
Kubernetes cluster carrying Ozone, and that is unwilling (or unable) to
mint principals with pod-FQDN hostnames.

## Layout

Two namespaces, both inside the same minikube cluster:

| Namespace | What's inside | Pretends to be |
| --- | --- | --- |
| `ozone-extkdc-edge` | MIT KDC + one client `Job` carrying `testuser`'s TGT | The corporate edge / user network |
| `ozone-extkdc-cluster` | OM, SCM, 3× DN, S3G | The Kubernetes-internal cluster |

Cross-namespace traffic is one-directional:

```
ozone-extkdc-edge   ──Kerberos──▶  om-public  (cluster, port 9862)
                    ──Sig V4──▶   s3g-public (cluster, port 9878)
```

The daemons in the cluster namespace **never** call into the edge
namespace except for the one-time keytab-load against the KDC at
startup (`interservice=false` keeps the rest of inter-service traffic
on SIMPLE auth).

## Principals seeded in the KDC

With `deploy.sh` (baseline), the realm carries:

```
testuser@EXAMPLE.COM
om/om@EXAMPLE.COM
s3g/s3g@EXAMPLE.COM
scm/scm@EXAMPLE.COM
dn/dn@EXAMPLE.COM
```

With `deploy-keyless.sh`, only these — every other principal the cluster
might once have needed has been eliminated:

```
testuser@EXAMPLE.COM
om/om@EXAMPLE.COM
```

The `om/om`, `s3g/s3g`, `scm/scm`, `dn/dn` entries are pre-baked in the
testkrb5 image. **None of them carry a hostname tied to a Kubernetes
pod, Service, or namespace FQDN** — that's the property the deployment
mode exists to honour. The KDC sees the cluster as a black box; the only
human-mintable principal added at deploy time is `testuser@EXAMPLE.COM`.

Daemon Kerberos configs use these short literal principals directly
(no `_HOST` substitution), so neither the OM's bind address nor the
client's connection target affects the SPN that Kerberos negotiates.

## What this proves about split-Kerberos

| Property | Evidence |
| --- | --- |
| Daemons start without per-pod-FQDN principals | OM/SCM/DN/S3G login lines: `Login successful for user om/om@EXAMPLE.COM` (etc.), zero KDC entries for `om-0`, `scm-0`, `dn-N` |
| The split-Kerberos advisory WARN fires across daemons | `WARN OzoneManager / StorageContainerManager / Gateway: Ozone is running in split-Kerberos mode (external=true, interservice=false); …` |
| OM's external client port speaks Kerberos | `SecurityLogger: Auth successful for testuser@EXAMPLE.COM (auth:KERBEROS) from <edge-pod-ip>:NNNN` |
| OM's service-RPC port speaks SIMPLE | `INFO OzoneManager: Bound OM service RPC server to om-0.om.ozone-extkdc-cluster.svc.cluster.local/…:9864 (auth=simple)` |
| Sig V4 traffic succeeds with no TGT | `aws --endpoint-url http://s3g-public...:9878 s3api create-bucket` returns 200 after `kdestroy` (Sig V4 → S3G → OM service-RPC SIMPLE → HMAC validation) |
| `ozone sh` is rejected without a TGT | Negative test in client Job — `ozone sh volume list` returns auth error after `kdestroy` |

## How to run

Two deploy variants, same manifests:

```bash
./deploy.sh           # baseline: every daemon carries its own keytab
./deploy-keyless.sh   # minimum-keytab: only OM (and client) have keytabs
```

`deploy.sh` distributes `om/om`, `scm/scm`, `dn/dn`, `s3g/s3g` keytabs to
the cluster namespace — this exercises the same path as `deploy.sh`
without the keyless refinement, so the SCM/DN/S3G keytabs are loaded
even though they're never used to authenticate anything in split mode.

`deploy-keyless.sh` is the deployment that the corporate-KDC use case
actually wants: the KDC mints **only** `testuser@EXAMPLE.COM` and
`om/om@EXAMPLE.COM`, and the cluster-side `keytabs` Secret contains
only `om.keytab`. SCM/DN/S3G start without ever opening
`/etc/security/keytabs/scm.keytab` (etc.) thanks to the per-daemon
gate split in Step J — they all check `requiresInterServiceKerberosLogin`
which is false when `interservice=false`. The deploy script prints
the actual keytabs directory contents on each pod as a sanity check.

Both scripts:

1. Create both namespaces and the KDC.
2. Seed the KDC with the required principals (4–5 for `deploy.sh`,
   2 for `deploy-keyless.sh`).
3. Extract keytabs via `kadmin.local`; create the cluster-side `keytabs`
   Secret and the edge-side `testuser-keytab` Secret.
4. Apply the daemon manifests in startup order: SCM → OM → DN → S3G.
5. Launch the client `Job` in the edge namespace and tail its logs.

The Job's exit code is the verdict.

## Caveat — pre-existing `Constable` issue blocks PutObject

The Sber multi-raft write path (introduced by `[SDPOZN-1708]` /
`[SDPOZN-1979]`) emits `java.lang.constant.Constable` references that
the Java-11 runner image can't load — `aws s3 cp` and `ozone fs -put`
both hang with `NoClassDefFoundError`. This is **not** caused by
split-Kerberos and reproduces identically on a non-split build. The
client Job exercises the surface that's known to work end-to-end:

- Volume / bucket create (control plane via OM Kerberos port)
- `ozone fs -ls`
- `ozone s3 getsecret`
- AWS Sig V4 `create-bucket` + `s3 ls`

Two follow-ups make the write path green: rebuild the dist with
`mvn -Drelease=11` (full Java-11-target cross-compile, not just
`-source 8 -target 8`) or bump the runner image to Java 17 LTS.

## Step P regression: SPNEGO ranger-admin + TGT renewal

### Why

Step L (commit ab99ec2d46) unconditionally flips
`Krb5LoginModule.isInitiator=false` whenever the daemon is in split-
Kerberos external mode with internal SIMPLE. The JVM loads its keytab
key for accepting SPNEGO/RPC service tickets but never acquires a TGT.

That is the intended behaviour for daemons whose only Kerberos surface
is an acceptor — Step L's "zero KDC traffic" property. But when the
same JVM has an in-process *outbound* Kerberos consumer (the Ranger
plugin's REST poll under SPNEGO), it has nothing to sign with. OM
keeps 401-ing ranger-admin and the ACL policy table freezes at the
boot snapshot — observed in the QA cluster `iftdrpoznb2c_ozone`
PolicyRefresher logs.

Step P (this commit) adds
`ozone.security.kerberos.acceptor-only.enabled` (default `true` keeps
Step L). The config in `30-config-configmap.yaml` sets it to `false`
so OM (and every other daemon) does a normal AS-REQ at startup +
Hadoop's auto-renewer.

### Build the OM image with the Ranger plugin

`spnego-ranger-om:dev` is built by `Dockerfile.spnego-ranger-om`. Only
OM uses this image — SCM, DN, S3G, Recon keep `ozone-split-kerberos:dev`.

```bash
cd /work/sber/component-ranger-plugins
mvn -pl :ranger-distro,:ranger-ozone-plugin,:ranger-ozone-plugin-shim,\
:ranger-plugin-classloader,:ranger-plugins-common,\
:ranger-plugins-audit,:ranger-plugins-cred \
  -am -P sdp-build-ranger-ozone-plugin package -DskipTests -q

cp target/ranger-2.4.0-ozone-plugin.tar.gz \
   /work/sber/component-ozone/hadoop-ozone/dist/src/main/k8s/examples/ozone-split-kerberos-external-kdc/

cd /work/sber/component-ozone/hadoop-ozone/dist/src/main/k8s/examples/ozone-split-kerberos-external-kdc
docker build -t spnego-ranger-om:dev -f Dockerfile.spnego-ranger-om .
minikube image load spnego-ranger-om:dev
```

### Run

`deploy.sh` now also applies `60-ranger-db.yaml`, `61-ranger-admin.yaml`,
`62-ranger-bootstrap.yaml`, `64-ranger-ozone-security-configmap.yaml`,
and runs `63-tgt-renewal-test.yaml` after the existing client Job.

The test:

1. Waits 120s past two ticket lifetimes
   (`ticket_lifetime=5m`, see `20-krb5-configmap.yaml`).
2. Pulls the `ozonedev` policy bundle from ranger-admin via basic auth
   to confirm the service is registered with `policy.download.auth.users`
   = `om,admin` (without that, OM gets 403 on its SPNEGO poll).
3. Extends the default ranger policy to grant `testuser`, waits one
   plugin poll cycle (5s × 3 + buffer), then proves `testuser` can
   create a volume.

Final line: `RESULT: TGT renewal + SPNEGO round-trip verified`.
