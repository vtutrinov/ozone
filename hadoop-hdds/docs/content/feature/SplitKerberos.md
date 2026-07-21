---
title: "Split-Kerberos"
weight: 11
menu:
   main:
      parent: Features
summary: Run Ozone with Kerberos required only on external client surfaces, while inter-service RPC and storage traffic stay on SIMPLE auth. The mode is purpose-built for Kubernetes deployments where a corporate KDC cannot mint principals for pod-FQDN hostnames.
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Motivation

`ozone.security.enabled=true` historically turned three things on at the
same time:

1. Kerberos login (and renewal) on every daemon.
2. SASL/Kerberos enforcement on every RPC port — including inter-service ports.
3. SPNEGO / AWS Sig V4 on the external surfaces.

That single switch breaks on Kubernetes when the corporate KDC is unable
or unwilling to mint principals with pod-FQDN hostnames
(`om-0.om.<ns>.svc.cluster.local` and friends). Pod names change on
restart, and the KDC team typically refuses to mint principals tied to
ephemeral hosts.

The **split-Kerberos** mode replaces the monolith with three orthogonal
flags so the cluster can run with **Kerberos only on the external
surfaces**, while inter-service traffic stays on SIMPLE auth — relying
on a service mesh (Istio, Linkerd) or NetworkPolicies for inter-pod
authenticity.

## Configuration

| Key | Default | Effect when `true` |
|---|---|---|
| `ozone.security.enabled` | `false` | Master switch / fallback default for the other two. |
| `ozone.security.kerberos.external.enabled` | inherits `ozone.security.enabled` | Kerberos required on OM's external client RPC (ofs/o3fs, `ozone s3 getsecret`) **and** on SCM's external admin port (`ozone admin scm/safemode/datanode`). S3G's SPNEGO. |
| `ozone.security.kerberos.interservice.enabled` | inherits `ozone.security.enabled` | Kerberos required on OM/SCM/DN/Recon's inter-service RPC ports. When off, those ports answer SIMPLE; clients fall back via `ipc.client.fallback-to-simple-auth-allowed=true`. |

Block-token, container-token and delegation-token enforcement stays on
in every combination — HMAC, independent of Kerberos.

### Supported combinations

| external | interservice | Mode |
|:---:|:---:|---|
| false | false | Insecure (legacy). |
| true | true | Fully kerberized (legacy). |
| true | false | **Split-Kerberos.** External-only Kerberos; the deployment target this feature is built for. |
| false | true | **Refused at startup.** Block-token / delegation-token issuance to ofs clients would silently break, since DT issuance requires a Kerberos-authenticated caller on the external port. |

The startup gate lives in
`OzoneSecurityUtil.validateKerberosFlags(conf, log)`. It also auto-sets
`ipc.client.fallback-to-simple-auth-allowed=true` whenever `interservice=false`,
so operators don't need to remember the flag in their `core-site.xml`.

## Per-daemon behaviour in split mode (`external=true, interservice=false`)

| Daemon | Keytab needed | Loads keytab? | Notes |
|---|:---:|:---:|---|
| **OM** | yes — `om/<service>@REALM` | yes | Serves Kerberos on its external client RPC port (9862); SIMPLE on the inter-service RPC port (9864, see below). |
| **SCM** | yes — `scm/<service>@REALM` | yes | Serves Kerberos on its external admin RPC port (9860); SIMPLE on block (9863), datanode (9861) and security (9961) ports. |
| **Datanode** | no | no (Step J) | Has no external Kerberos surface. Its inbound traffic from SCM/OM is SIMPLE. Mounts the cluster Secret but never reads any keytab file. |
| **Recon** | no | no | Talks to OM/SCM only; both are reachable over SIMPLE. |
| **S3G** | no | no | Sig V4 is HMAC end-to-end. SPNEGO is optional and uses a separate `HTTP/*` keytab via the embedded Jetty if enabled. |

For OM and SCM, an additional optimisation flips Hadoop's
`Krb5LoginModule` into **acceptor-only** mode (Step L): the keytab is
read locally into the JAAS Subject but no AS-REQ is sent to the KDC,
and no TGT renewer thread is spawned. Net KDC traffic per pod over its
entire lifetime: **zero**.

## Per-port behaviour

### OM ports

| Port | Protocol | Default auth before split mode | In split mode |
|---|---|---|---|
| 9862 | `OzoneManagerProtocol` (external clients: ofs, `ozone sh`, `ozone s3 getsecret`) | Kerberos | **Kerberos** (unchanged). |
| 9864 | `OzoneManagerProtocol` mirror (sibling RPC server, used by S3G and Recon) | n/a | **SIMPLE**. New port; same protocols served, different SASL config. Built by `OzoneManager#maybeBuildServiceRpcServer`. |
| 9874 | HTTPS UI | SPNEGO | SPNEGO if enabled, otherwise plain HTTP. |

The two-port design mirrors HDFS NameNode's `clientRpcServer` /
`serviceRpcServer` pair; the novelty is varying the SASL profile per
server within the same JVM. Block-token / delegation-token
authorisation remains HMAC-verified on both ports.

### SCM ports

| Port | Protocol | Default auth before split mode | In split mode |
|---|---|---|---|
| 9860 | `StorageContainerLocationProtocol` (external admin: `ozone admin scm/safemode/datanode/container/pipeline`) | Kerberos | **Kerberos** (Step K). |
| 9866 | `StorageContainerLocationProtocol` sibling for inter-service callers (OM pipeline refresh) | n/a | **SIMPLE** (Step M). New sibling RPC server; built when `ozone.scm.service.rpc-address` is set. Same protocol, different SASL profile, just like OM's split-port. |
| 9863 | `ScmBlockLocationProtocol` (OM ↔ SCM block allocation) | Kerberos | SIMPLE. |
| 9861 | `StorageContainerDatanodeProtocol` (DN heartbeats and reports) | Kerberos | SIMPLE. |
| 9961 | `SCMSecurityProtocol` (cert issuance / secret-key vending — used by OM and DN internally) | Kerberos | SIMPLE. |

Why Step M had to exist: `StorageContainerLocationProtocol` is **both** an external admin surface *and* an inter-service surface. OM calls `getContainerWithPipelineBatch` on every key read that has to refresh pipeline info (see `KeyManagerImpl.refreshPipeline`). Step K kept 9860 on Kerberos to satisfy "Kerberos required for `ozone admin`"; Step L removed OM's TGT (acceptor-only). That left OM unable to talk to SCM on every read with `Failed to find any Kerberos tgt`. Step M adds a sibling SIMPLE port (default 9866) that internal callers route to via `HAUtils.getScmContainerClient(conf, ugi, internalCaller=true)`; OM uses that. External `ozone admin` keeps targeting 9860 over Kerberos.

## Request flows

### Flow 1 — `ozone fs -ls ofs://<om>/...` (external client → OM)

```
client pod                                          cluster pod
─────────                                           ───────────
1. kinit testuser
   ─AS-REQ──────────────────────────► KDC
   ◄────AS-REP (TGT)─────────────────
2. ozone fs -ls
   ─TGS-REQ (om/om@REALM)────────────► KDC
   ◄────TGS-REP (service ticket)─────
   ─AP-REQ (ticket + authenticator)──► OM:9862  (Kerberos)
                                                 OM decrypts using om/om
                                                 key from its keytab.
                                                 NO KDC contact server-side.
   ◄────list of buckets──────────────
```

### Flow 2 — `aws s3 cp` Sig V4 (external client → S3G → OM)

```
client pod                                          cluster pod
─────────                                           ───────────
1. testuser obtains S3 secret once (via Step 2 below).

2. aws s3 cp (signed with Sig V4 HMAC)
   ─HTTP PutObject──────────────────► S3G:9878    (NO Kerberos)
                                                 S3G validates Sig V4 by
                                                 looking up the user's S3
                                                 secret via OM service
                                                 port:
                                              S3G ─SIMPLE RPC──► OM:9864
                                              S3G ◄─secret──── OM
                                                 S3G validates HMAC
                                                 locally, then forwards
                                                 the write to OM/DN.
   ◄────HTTP 200────────────────────
```

NO KDC interaction in step 2. The entire data-plane round-trip is
HMAC-validated.

### Flow 3 — `ozone admin scm roles` (external admin → SCM)

```
client pod                                          cluster pod
─────────                                           ───────────
1. kinit (already done).

2. ozone admin scm roles
   ─TGS-REQ (scm/scm@REALM)──────────► KDC
   ◄────TGS-REP (service ticket)─────
   ─AP-REQ───────────────────────────► SCM:9860   (Kerberos, Step K)
                                                 SCM decrypts using
                                                 scm/scm key from keytab.
                                                 NO KDC contact server-side.
   ◄────role list────────────────────
```

Without a TGT the same command is rejected with a Kerberos-shaped
`AccessControlException: Client cannot authenticate via:[KERBEROS]`.

### Flow 4 — OM → SCM block allocation (inter-service)

```
OM pod                                              SCM pod
──────                                              ───────
ozone client requests key allocation.
OM needs blocks → connects to SCM:9863.

OM IPC client (UGI is Kerberos, no TGT) advertises Kerberos.
SCM:9863 (cloned conf with hadoop.security.authentication=simple)
        responds: "I support SIMPLE only".

With ipc.client.fallback-to-simple-auth-allowed=true the OM client
downgrades to SIMPLE. NO Kerberos handshake, NO KDC contact, NO TGT
required.

OM ──RPC (SIMPLE, user=om)──► SCM:9863
SCM ◄─block allocation─── OM
```

### Flow 5 — DN heartbeat (inter-service)

```
DN pod (no keytab, UGI = OS user `hadoop`)         SCM pod
──────                                              ───────
DN ──TCP connect──► SCM:9861
   (DN UGI is SIMPLE because Step J skipped the keytab login.)
DN ──RPC (SIMPLE, user=hadoop)──► SCM:9861
SCM accepts, processes heartbeat.
```

`hadoop.security.authorization=false` is set on the cluster so SCM
doesn't enforce the `@KerberosInfo.clientPrincipal` ACL on DN's SIMPLE
request — network-layer auth (service mesh / NetworkPolicy) is the
substitute.

## Keytab and KDC footprint

In the keyless reference deployment:

```
KDC principals (the entire realm content for this cluster):
    testuser@EXAMPLE.COM
    om/om@EXAMPLE.COM
    scm/scm@EXAMPLE.COM

Cluster-side Secret (`keytabs`):
    om.keytab         ── used by OM
    scm.keytab        ── used by SCM
    (no dn.keytab, no s3g.keytab, no recon.keytab)

Edge-side Secret (`testuser-keytab`):
    testuser.keytab   ── used by the client Job
```

Three principals minted by the KDC. Two keytabs on Ozone pods. None of
them are tied to k8s pod FQDNs.

### KDC interaction matrix

| Source | Destination | When | Why |
|---|---|---|---|
| testuser client | KDC | once per `kinit` | AS-REQ for TGT |
| testuser client | KDC | once per *new* service | TGS-REQ for the service ticket (`om/om` or `scm/scm`) |
| testuser client | OM:9862 or SCM:9860 | once per RPC connection | AP-REQ sent in the SASL handshake. The server decrypts locally — KDC not involved. |
| OM pod | KDC | **never** | Step L flips `isInitiator=false`; the keytab is loaded into the Subject without an AS-REQ. |
| SCM pod | KDC | **never** | Same. |
| DN pod | KDC | **never** | Step J skips the keytab login entirely. |
| S3G pod | KDC | **never** | Same. |

## How to monitor that the design actually holds

### 1. Daemon-side proof that the AS-REQ is suppressed

Step L logs one INFO line per daemon at startup. Grep for it:

```bash
$ kubectl -n ozone-extkdc-cluster logs om-0 \
    | grep "isInitiator switched"
INFO org.apache.hadoop.ozone.om.OzoneManager:
  Krb5LoginModule isInitiator switched: null -> false.
  Daemon will load its keytab without contacting the KDC;
  no AS-REQ at startup, no TGT renewer.

$ kubectl -n ozone-extkdc-cluster logs scm-0 \
    | grep "isInitiator switched"
INFO org.apache.hadoop.hdds.scm.server.StorageContainerManager:
  Krb5LoginModule isInitiator switched: null -> false. ...
```

Both daemons should also log the split-Kerberos advisory:

```bash
$ for p in om-0 scm-0 datanode-0 s3g-0 ; do
    kubectl -n ozone-extkdc-cluster logs $p \
      | grep -m1 "split-Kerberos mode"
  done
WARN OzoneManager:        Ozone is running in split-Kerberos mode
                          (external=true, interservice=false); ...
WARN StorageContainerManager: ...
WARN HddsDatanodeService:     ...
WARN Gateway:                 ...
```

### 2. KDC log audit — zero AS-REQ from cluster daemons

The MIT KDC logs every AS-REQ and TGS-REQ. Group them by principal:

```bash
$ KDC_POD=$(kubectl -n ozone-extkdc-edge get pod \
    -l component=kdc -o jsonpath='{.items[0].metadata.name}')

$ kubectl -n ozone-extkdc-edge logs "$KDC_POD" \
    | grep "AS_REQ" \
    | sed -E 's/.*ISSUE: authtime [^,]+, etypes \{[^}]*\}, ([^ ]+) for .*/\1/' \
    | sort | uniq -c
    1 testuser@EXAMPLE.COM    ← only the client kinit

$ kubectl -n ozone-extkdc-edge logs "$KDC_POD" \
    | grep "TGS_REQ" \
    | sed -E 's/.*, ([^ ]+) for ([^ ]+).*/  \1  →  \2/' \
    | sort | uniq -c
    5 testuser@EXAMPLE.COM  →  om/om@EXAMPLE.COM
    3 testuser@EXAMPLE.COM  →  scm/scm@EXAMPLE.COM
```

Anything labelled `om/om@EXAMPLE.COM` or `scm/scm@EXAMPLE.COM` in the
*AS_REQ* column would indicate the daemon contacted the KDC — that's
the regression signal to watch for.

### 3. Network-level proof — packet capture between cluster and KDC

If the daemon-side log is doctored, the network never lies. From a
host that can reach the KDC's Service IP:

```bash
$ kubectl -n ozone-extkdc-edge exec deploy/kdc -- \
    timeout 60 tcpdump -nn -i any -c 50 'port 88'
# (Run the full deploy, run the client Job, watch the output.)
```

Expected: packets only from the **edge namespace** (client + kdc
sidecar). No traffic from the `ozone-extkdc-cluster` namespace
addresses.

Or enforce it with a NetworkPolicy that denies cluster-namespace
egress to the KDC Service — the daemons should keep working:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-kdc-egress-from-cluster
  namespace: ozone-extkdc-cluster
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ozone-extkdc-cluster
  - to:
    - ipBlock:
        cidr: 0.0.0.0/0
        except:
        - 10.96.0.0/12   # KDC ClusterIP range; substitute yours
```

### 4. Proof that external clients DO authenticate via Kerberos

OM logs every successful (and failed) Kerberos handshake at INFO via
`SecurityLogger`:

```bash
$ kubectl -n ozone-extkdc-cluster logs om-0 \
    | grep "Auth successful"
INFO SecurityLogger.org.apache.hadoop.ipc.Server:
  Auth successful for testuser@EXAMPLE.COM (auth:KERBEROS)
  from 10.244.0.198:48394 / 10.244.0.198:48394
```

`(auth:KERBEROS)` is the load-bearing token — it means the client's
service ticket was decrypted and the authenticator validated. The
remote address is the client pod's IP in the edge namespace.

Negative test: after the client runs `kdestroy`, the same operation
should be rejected with a Kerberos-shaped error:

```bash
$ kubectl -n ozone-extkdc-edge exec debug-client -- \
    bash -c "kdestroy ; ozone admin scm roles 2>&1"
... AccessControlException: Client cannot authenticate via:[KERBEROS]
```

### 5. Proof that inter-service ports are SIMPLE

Inspect the SCM RPC server's announced auth list. Easiest via JMX, but
the same information shows up in startup logs:

```bash
$ kubectl -n ozone-extkdc-cluster logs scm-0 \
    | grep -E "RPC server for|listening"
INFO StorageContainerManager: StorageContainerLocationProtocol RPC server
  is listening at /0.0.0.0:9860            ← Kerberos in split mode
INFO SCMClientProtocolServer: RPC server for Client is listening at
  /0.0.0.0:9860
INFO StorageContainerManager: ScmBlockLocationProtocol RPC server is
  listening at /0.0.0.0:9863               ← SIMPLE in split mode
INFO SCMBlockProtocolServer: RPC server for Block Protocol is listening
  at /0.0.0.0:9863
INFO SCMSecurityProtocolServer: Starting RPC server for
  SCMSecurityProtocolServer. is listening at /0.0.0.0:9961  ← SIMPLE
INFO SCMDatanodeProtocolServer: ScmDatanodeProtocol RPC server for
  DataNodes is listening at /0.0.0.0:9861  ← SIMPLE
```

When a SIMPLE connection lands on a SIMPLE port, the security log
shows no `(auth:KERBEROS)` — the user identity is the OS user the
remote caller is running as:

```bash
$ kubectl -n ozone-extkdc-cluster logs scm-0 \
    | grep -E "Auth successful for hadoop"
INFO SecurityLogger.org.apache.hadoop.ipc.Server:
  Auth successful for hadoop (auth:SIMPLE) from ...
```

That entry is the inter-service traffic from DN or OM landing on
SCM's block / datanode / security ports.

### 6. Negative test on the SCM external port

`ozone admin scm` from a client that does **not** hold a TGT must be
rejected — this is the regression guard against accidentally dropping
SCM's external port back to SIMPLE:

```bash
$ kubectl -n ozone-extkdc-edge exec debug-client -- bash -c "
    kdestroy ; klist ;
    ozone admin scm roles 2>&1 | tail -3
"
klist: No credentials cache found (filename: /tmp/krb5cc_1000)
... AccessControlException: Client cannot authenticate via:[KERBEROS]
```

If the same command succeeds without a TGT, Step K has regressed.

## Reference deployment

`hadoop-ozone/dist/src/main/k8s/examples/ozone-split-kerberos-external-kdc/`
contains a runnable example exercising every property above:

- Two namespaces (`ozone-extkdc-edge` for KDC + client, `ozone-extkdc-cluster` for daemons).
- The KDC is seeded with only three principals — `testuser@`, `om/om@`, `scm/scm@`.
- The cluster `keytabs` Secret contains only `om.keytab` and `scm.keytab`.
- A client `Job` exercises the full external surface and asserts the negative auth cases.

```bash
# Apply the example:
./hadoop-ozone/dist/src/main/k8s/examples/ozone-split-kerberos-external-kdc/deploy-keyless.sh

# Sanity-check the Job:
kubectl -n ozone-extkdc-edge logs job/ozone-client | tail -20
```

A docker-compose smoke test lives at
`hadoop-ozone/dist/src/main/compose/ozonesecure-split/`. The Robot
smoketest at `hadoop-ozone/dist/src/main/smoketest/security/split-kerberos.robot`
covers:

| Case | What it asserts |
|---|---|
| External ofs succeeds with TGT, fails without | OM external Kerberos surface |
| External ofs write + read round-trip with TGT | ofs data-plane works in split mode |
| External ofs write without TGT is rejected | Negative auth on OM external |
| S3 secret round-trip | `ozone s3 getsecret` works under Kerberos |
| External AWS S3 sigv4 succeeds without TGT | Sig V4 / HMAC path |
| OM rejects forged S3 signatures | HMAC validation works |
| SCM admin succeeds with TGT | SCM external Kerberos surface (Step K) |
| SCM admin without TGT is rejected | Negative auth on SCM external |

## Migration playbooks

Once a cluster is running with pod-FQDN principals, two scripts help
migrate to the stable-name principals split-Kerberos prefers:

- `migrate-to-stable-principal.sh` — OM: per-pod `om/_HOST@REALM` →
  stable `om/<service>@REALM`. Four phases (`preflight`, `roll`,
  `config`, `cleanup`), each gated behind an interactive confirmation
  and reversible until cleanup.
- `migrate-scm-to-stable-principal.sh` — same for SCM:
  `scm/_HOST@REALM` → `scm/<service>@REALM`.

Both scripts:
1. Pre-flight check the cluster state and back up the existing Secret
   and ConfigMap.
2. Roll the merged keytab (per-pod entry **and** stable-name entry)
   to every pod, with the leader rolled last.
3. Switch the daemon's `kerberos.principal` config to announce the
   stable name. Per-pod entries stay in the keytab as a safety net.
4. After every client has been re-pointed, retire the per-pod
   principals from the keytab and from the KDC.

## Known caveats

- The ofs PutObject path on the Sber multi-raft fork still hits a
  pre-existing `NoClassDefFoundError: java/lang/constant/Constable`
  on the Java-11 runner image (unrelated to split-Kerberos). The
  example Job and Robot test deliberately skip the write step on
  that path; AWS Sig V4 reads/lists work end-to-end.
- 8 of 10 Ratis-TLS gate sites still check `isSecurityEnabled` rather
  than `isInterServiceKerberosEnabled`. The recommended workaround
  for split deployments is `hdds.grpc.tls.enabled=false` — both the
  example configmap and the docker-compose env set it. A future
  patch should make every gate consistent.
- HA-aware OmTransport address rewriting is single-address only.
  S3G / Recon dialing the SIMPLE service port assumes a non-HA OM.
  Adding HA support is a follow-up.

## Reference commits

The implementation lives on `feature/split-kerberos`. Each step is a
single revertable commit. The order, with what each contains:

| Step | Commit | What it adds |
|---|---|---|
| A | foundation | Two new config keys + accessors; `OzoneSecurityUtil.validateKerberosFlags`. |
| B | daemon login gates | Each daemon's startup login routed through `requiresDaemonKerberosLogin`. |
| C-1 | OM split-port RPC | `omServiceRpcServer` sibling RPC server with SIMPLE SASL profile. |
| C-2 | `OmTransportFactory` internal flag | S3G + Recon flipped to the SIMPLE service port. |
| D | SCM inter-service SASL gates | Block / datanode / security ports clone conf with SIMPLE when interservice=false. |
| E | delegation token op gate | `isAllowedDelegationTokenOp` keyed off external Kerberos. |
| H-1..H-4 | tests + reference deployments | `TestSecureOzoneCluster` parametrisation, docker-compose, k8s examples. |
| J | per-daemon login refinement | DN/Recon/S3G gated on inter-service only. SCM/OM stay on the composite gate. |
| K | SCM external admin port stays Kerberos | `SCMClientProtocolServer` keeps Kerberos when external=true regardless of interservice. |
| L | acceptor-only Kerberos mode | OM and SCM never contact the KDC at startup or for renewal. |
| M | SCM split-port for `StorageContainerLocationProtocol` | Sibling SIMPLE-auth RPC server (default port 9866) for OM and other internal callers, so the Kerberos-required 9860 port can stay for external `ozone admin` without forcing OM to hold a TGT. Mirrors OM's Step C-1 / C-2 pattern on the SCM side. |

