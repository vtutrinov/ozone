<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
-->

# Ozone split-Kerberos example

This example deploys a single-OM Ozone cluster in the **split-Kerberos** mode:

- `ozone.security.kerberos.external.enabled=true` — ofs/o3fs RPC to OM, the
  S3 Gateway HTTP listener (SPNEGO + AWS Sig V4), and `ozone s3 getsecret`
  all require a Kerberos identity.
- `ozone.security.kerberos.interservice.enabled=false` — OM↔SCM, SCM↔DN, and
  S3G→OM run over a SIMPLE-auth port. The expectation is that the
  Kubernetes platform provides inter-service authenticity below the Ozone
  layer (Istio mTLS, network policy, etc.).

The motivation is operating against a corporate KDC that cannot mint
principals for Kubernetes pod FQDNs (`om-0.om.default.svc.cluster.local`).
In this deployment the daemons still load a keytab — they need it for the
external-facing SPN — but **no inter-service SPN is required** because the
service-RPC port (`ozone.om.service.rpc-address=om-0.om:9864`) carries
SIMPLE auth.

## Known limitation

The `OmTransportFactory` address-rewrite path used by S3G and Recon
currently supports only the single-OM topology. Once HA is added, a
per-node service-RPC address map will be needed; that work is tracked
separately. This example deploys a single OM accordingly.

## What's inside

| File | Purpose |
|---|---|
| `config-configmap.yaml` | Cluster-wide Ozone config. Sets the two new flags + `ozone.om.service.rpc-address`. |
| `kdc.yaml` | KDC pod (`apache/ozone-testkrb5`). Seeds principals only for `testuser`, `HTTP/*`, and the daemons' external SPNs. |
| `keytabs-secret.yaml` | Placeholder Secret. The test workflow assumes you populate it from the KDC pod (see the README in the manifest). |
| `om-statefulset.yaml` + `om-service.yaml` | OM, exposing client port `9862` (Kerberos) and the new service port `9864` (SIMPLE). |
| `scm-statefulset.yaml` + `scm-service.yaml` | SCM. |
| `datanode-statefulset.yaml` + `datanode-service.yaml` | Datanodes. |
| `s3g-statefulset.yaml` + `s3g-service.yaml` | S3 Gateway. |
| `ozone-client-job.yaml` | One-shot Job that mounts the `testuser` keytab, kinits, and exercises `ozone fs -ls ofs://` plus `ozone s3 getsecret` and `aws s3 ls`. Its exit code is the test verdict. |
| `istio-peerauthentication.yaml` | Optional Istio manifest applying STRICT mTLS to the namespace. Documents the deployment assumption rather than gating the Job. |
| `kustomization.yaml` | Aggregates everything. |

## Deploying

```bash
kubectl apply -k hadoop-ozone/dist/src/main/k8s/examples/ozone-split-kerberos

# Wait for KDC + OM + SCM + DN to come up, then seed the keytab Secret.
# The Job triggers on its own and prints PASS/FAIL.
kubectl logs job/ozone-client
```

The Job's exit code is the integration-test verdict. Look in
`kubectl logs deployment/om` for the operator-facing WARN line
`split-Kerberos mode (external=true, interservice=false)` emitted by
`OzoneSecurityUtil.validateKerberosFlags`.

## What this proves

1. Daemons start and reach the running state with **no inter-service
   Kerberos handshakes** on the cluster network.
2. A client outside the daemons can still do the full ofs + S3 flow over
   Kerberos against the dedicated external port.
3. The `validateKerberosFlags` advisory log fires at startup, confirming the
   operator landed in the intended deployment mode (not an
   unsupported combination).
