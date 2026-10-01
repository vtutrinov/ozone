---
title: "Upgrading from SDP Ozone 1.4"
weight: 12
menu:
   main:
      parent: Features
summary: "How to upgrade a cluster running SDP Ozone 1.4 to SDP Ozone 2.2.1 (Apache Ozone 2.2.1 plus the SDP features): the offline OM DB migration of the SDP proto fields, compatibility notes and renamed commands and settings."
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

SDP Ozone 2.2.1 is Apache Ozone 2.2.1 with the SDP features on top. Upgrading from SDP Ozone 1.4 is the
upstream 1.4 → 2.x upgrade (see the upstream upgrade documentation, including layout finalization) plus the
SDP-specific steps below.

## Why an extra step is needed

SDP 1.4 added its own protobuf fields, enum values and operations with the next free numbers. Upstream Ozone
later used several of these numbers for its own fields. On the 2.x line every SDP number therefore lives in the
**1000+ range**, which keeps SDP wire- and disk-compatible with Apache Ozone and future upstream releases.

Two persisted messages carry SDP fields, so an OM DB written by SDP 1.4 must be migrated once:

| Message | SDP field | SDP 1.4 | 2.x | Upstream 2.x meaning of the old number |
|---|---|---|---|---|
| KeyInfo | compressionType | 20 | 1001 | ownerName (string) |
| KeyInfo | originalDataSize | 21 | 1002 | tags (repeated KeyValue) |
| KeyInfo | ownerName (SDPOZN-2800) | 22 | 20 (upstream ownerName) | expectedDataGeneration (uint64) |
| BucketInfo | compressionType | 21 | 1001 | snapshotUsedBytes (uint64) |
| BucketInfo | raftGroup (multi-raft) | 22 | 1002 | snapshotUsedNamespace (uint64) |

Without the migration Ozone 2.x reports the compression codec of a key as its **owner** and silently drops the
other SDP fields.

## Procedure

1. Upgrade all components of a cluster together. SDP operations are not compatible between SDP 1.4 and 2.x
   (see below), so a rolling upgrade from 1.4 is not supported.
2. Prepare the OMs: `ozone admin om prepare -id <om-service-id>`. With multi-raft enabled, make sure all bucket
   raft groups are flushed as well (the prepare index covers the main OM raft group).
3. Stop all OMs, then on every OM node:
   ```shell
   ozone repair om sdp-proto-migrate --db <ozone.om.db.dirs>/om.db --dry-run
   ozone repair om sdp-proto-migrate --db <ozone.om.db.dirs>/om.db
   ```
   The dry run prints the number of records per message type and the compression types it moves to field 1001;
   check that they are codec names. Repeat both steps for every OM snapshot checkpoint DB, if snapshots are used.
   The tool marks the DB as migrated and refuses to run twice.
4. Start the new version and finalize the upgrade as described upstream.

SCM and datanode metadata contain no SDP fields; nothing needs to be migrated there.

## Compatibility notes

* **fscheck / VerifyBlock**: the datanode `VerifyBlock` operation moved from 21 to 1001 (21 is upstream
  `FinalizeBlock`). Never run the SDP 1.4 `fscheck` against upgraded datanodes.
* **S3 Gateway and OM must have the same version**: SDP 1.4 sent the multipart part number in `KeyArgs` field
  24, which is `expectedETag` in 2.x. Part-aware GET is now upstream (HDDS-11699).
* SDP OM operations (content summary, multi-raft, rate limiters) and statuses use new numbers; SDP 1.4 clients
  and servers cannot call them on 2.x.
* Follower read and multi-raft cannot be enabled together.

## Renamed commands and settings

| SDP 1.4 | SDP 2.2.1 |
|---|---|
| `ozone fscheck` (tools) | `ozone admin fscheck` |
| rate limiter CLI | `ozone admin ratelimiter create\|delete\|list` |
| `ozone admin om refresh-usedbytes <vol>/<bucket>` | `ozone repair om quota start --buckets /<vol>/<bucket>` (upstream quota repair) |
| `ozone repair om transaction-info` | `ozone repair om update-transaction --db <om.db> --term T --index I` (upstream) |
| `ozone repair om raft-log inspect\|truncate` | unchanged |
| `ozone.om.follower.read.enabled` | upstream follower read: client `ozone.client.follower.read.enabled`, `ozone.client.follower.read.default.consistency=LOCAL_LEASE`; OM `ozone.om.follower.read.local.lease.enabled` |
| `dfs.datanode.use.datanode.hostname` | `hdds.datanode.use.datanode.hostname` (the old key still works as a deprecated alias) |
| — | `ozone.om.rpc-bind-host` (new, default `0.0.0.0`: the OM RPC server binds to all interfaces; `ozone.om.address` stays the advertised address) |
