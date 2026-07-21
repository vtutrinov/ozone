<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ozone-with-ranger-freeipa

End-to-end test of the Ranger Ozone plugin's IPA user/group sync (SDPOZN-2434).

Boots, in a single kind cluster:

* Ozone (single OM / SCM / DataNode / S3G), built from this repo, with the
  `ranger-ozone-plugin` and `ranger-ipa-userstore` (from
  `/work/sber/component-ranger-plugins`) dropped into `/opt/ozone/share/ozone/lib/`
* Ranger Admin 2.4.0 (HTTPS + basic auth), embedded Solr
* FreeIPA (realm `EXAMPLE.TEST`), bootstrapped with users `testuser` / `testuser2`
  and groups `group1` / `group2`, where `testuser` belongs to `group1`
* A test Job that runs the eight ACL outcomes from the SDPOZN-2434 spec:

  | Identity   | Resource         | Op  | Expected |
  | ---------- | ---------------- | --- | -------- |
  | testuser   | vol1/bucket1     | rw  | allow    |
  | testuser   | vol1/bucket2     | r   | allow    |
  | testuser   | vol1/bucket2     | w   | deny     |
  | testuser   | vol1/bucket3     | r/w | deny     |
  | testuser2  | * / *            | all | allow    |

`testuser2` is the cluster super-user via a global Ranger policy.

## Quickstart

```
make image            # build Ozone + Ranger plugin tarball, load into kind
make up               # kind create cluster && kubectl apply -k .
make test             # wait for acl-test Job, dump its logs, exit with its status
make down             # tear the cluster down
```

## What proves the IPA sync works

If `group1` membership were resolved against the container OS (no NSS for IPA)
the bucket2 read would be denied for `testuser`. The fact that the test passes
means `RangerOzoneAuthorizer` saw `group1` in `ugi.getGroupNames()` for the
`testuser` UGI — i.e. the `IpaGroupMapping` (loaded via
`hadoop.security.group.mapping`) returned the IPA-fetched group set.

## Knobs

`ranger-policies-job.yaml` is the only place that names the policies. To add a
sanity scenario, append to its `POLICIES=` JSON array — the ACL Job picks up
new buckets if you add matching `expect_*` lines to `acl-test-job.yaml`.

## Known TODOs before this runs green end-to-end

* **`Dockerfile.ranger-admin`** is a placeholder. The community has not
  published a `ranger-admin:2.4.0` image; wire one of: (a) a locally built
  `security-admin` tarball, (b) a third-party image such as
  `gbevan/ranger-admin`, or (c) the dropp-app Ranger image if the team
  publishes it.
* FreeIPA needs ~3 minutes to bootstrap on first start — `make test` waits
  20 minutes for `job/freeipa-bootstrap`, but raise it if your machine is
  slow.
* The `keytabs` Secret is created by `keytab-bootstrap-job`. OM/SCM/DN/S3G
  pods will sit in `ContainerCreating` until that Job completes; this is
  expected, not a bug.
