---
title: "OM Bucket Raft Groups (multi-raft)"
weight: 13
menu:
   main:
      parent: Features
summary: "SDP: bucket write requests replicated by several OM raft groups (ozone.om.multi.raft.bucket.enabled): group lifecycle, object IDs, read consistency and follower read."
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

With `ozone.om.multi.raft.bucket.enabled=true`, the OMs run `ozone.om.multi.raft.bucket.groups` bucket raft groups
besides the OM raft group. Each bucket is assigned to one group on its first key write (the assignment is
replicated through the OM raft group and persisted in the bucket info), and the key, file and multipart upload
writes of the bucket are replicated by that group. Volume and bucket operations, key ACL changes and the
background services (key purge, open key cleanup) stay in the OM raft group. All groups apply to the same OM DB.

## Lifecycle

* The bucket raft groups survive OM restarts: each group keeps its Ratis log and its own TransactionInfo in the
  OM DB, and continues from them, like the OM raft group. A change of `ozone.om.multi.raft.bucket.groups` adds
  groups on restart; it does not remove any.
* The leader of the OM raft group creates missing groups and replaces groups that are closed. A group without a
  leader (e.g. electing after a restart) or with an unhealthy peer is kept: Raft commits with a majority and the
  peers catch up from the log.
* Every bucket raft group id carries a **serial**, unique over the lifetime of the cluster: the highest serial is
  replicated through the OM raft group, and a group is never re-created under an id used before. Up to 1022
  bucket raft groups can be created over the lifetime of a cluster.

## Object and update IDs

A transaction of a bucket raft group generates its object and update IDs from `serial << 44 | log index`; the OM
raft group (serial 0) uses its log index. The groups never generate the same IDs, which matters in particular for
FSO buckets, where object IDs are the parent references of files and directories. With multi-raft enabled, a raft
group accepts 2^44 transactions (writes fail beyond).

The updateIDs one raft group generates grow monotonically and the OM checks it (also after multi-raft is switched
off). Updates of the same object by
different raft groups (e.g. a key written by its bucket raft group, then its ACL changed through the OM raft group)
are not ordered, and the check does not apply between them.

## Read consistency

A read of keys, files or multipart uploads of a bucket (`LookupKey`, `GetKeyInfo`, `ListKeys`, `LookupFile`,
`GetFileStatus`, `ListStatus`, `ListMultipartUploads`, `ListMultiPartUploadParts`, `GetObjectTagging`) is served
under the leadership, lease or ReadIndex of the bucket raft group of the bucket, not of the OM raft group; other
reads use the OM raft group. The client sends the bucket requests to the OM leading the group of the bucket and
follows `OMNotLeaderException` of the group.

* Leader reads (default): the leader of the bucket raft group serves its reads. The state of the OM raft group
  (volumes, buckets, key ACLs) it reads may lag behind the OM raft group leader by the replication delay.
* `ozone.om.ha.raft.server.read.option=LINEARIZABLE`: an OM serves a bucket read after the ReadIndex of the bucket
  raft group, and of the OM raft group if it is not its leader, so the read is linearizable.
* Follower read (`ozone.client.follower.read.enabled=true`) works with multi-raft: the reads go through the follower
  read proxy provider, the writes to the group leaders. The OMs need the `LINEARIZABLE` read option (or the local
  lease, `ozone.om.follower.read.local.lease.enabled`, which then checks the lag of both groups); otherwise a
  follower answers with `OMNotLeaderException`.

## Limitations

* Up to 1022 bucket raft groups over the lifetime of a cluster, and 2^44 transactions per raft group.
* A closed group is removed on all OMs and replaced; transactions it committed but not yet applied on the remaining
  OMs are lost. Its buckets are assigned again on their next write.
* When multi-raft is switched off, the OMs remove the bucket raft groups on start; transactions a group committed
  but did not apply yet are lost, so switch it off with the writes stopped. Bucket raft groups of SDP 1.4 (ids
  without serial) are removed on start as well, see [Upgrading from SDP Ozone 1.4]({{< ref "SdpUpgradeFrom14.md" >}}).
