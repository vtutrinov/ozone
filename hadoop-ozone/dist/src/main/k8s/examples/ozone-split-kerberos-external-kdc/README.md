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

The KDC's `EXAMPLE.COM` realm carries only these identities (apart from
the `apache/ozone-testkrb5` image's own bookkeeping):

```
testuser@EXAMPLE.COM
om/om@EXAMPLE.COM
s3g/s3g@EXAMPLE.COM
scm/scm@EXAMPLE.COM
dn/dn@EXAMPLE.COM
```

The `om/om`, `s3g/s3g`, `scm/scm`, `dn/dn` entries come pre-baked in the
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

```bash
./deploy.sh
```

The script:

1. Creates both namespaces.
2. Brings up the KDC; adds `testuser@EXAMPLE.COM`; extracts keytabs
   for the five identities listed above.
3. Creates the `keytabs` Secret in the cluster namespace (om/s3g/scm/dn
   keytabs) and the `testuser-keytab` Secret in the edge namespace.
4. Applies the cluster ConfigMap, then the StatefulSets in startup order:
   SCM → OM → DN → S3G.
5. Launches the client `Job` in the edge namespace and tails its logs.

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
