/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRatisGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRatisGroupManager.class);

  private final int omRatisGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;

  private final Map<String, UUID> bucketRatisGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> ratisGroupCounter = new ConcurrentHashMap<>();

  public OmRatisGroupManager(
      OzoneConfiguration configuration,
      boolean multiRaftEnabled,
      String omServiceId,
      OMMetadataManager metadataManager
  ) {
    omRatisGroupCount = configuration.getInt(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServiceId = omServiceId;
    this.metadataManager = metadataManager;

    initBucketMap();
  }

  private void initBucketMap() {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator =
        metadataManager.getBucketIterator();
    while (bucketIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo != null) {
        UUID raftGroup = bucketInfo.getRaftGroup();
        if (raftGroup != null) {
          String key = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
          bucketRatisGroups.put(key, raftGroup);
          ratisGroupCounter.compute(raftGroup, (k, v) -> v == null ? 1 : v + 1);
        }
      }
    }
  }

  public synchronized RaftGroupId ratisGroupName(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return RaftGroupId.valueOf(toUuid(omServiceId));
    }

    String key = metadataManager.getBucketKey(volumeName, bucketName);
    UUID storedUuid = bucketRatisGroups.get(key);
    if (storedUuid != null) {
      return RaftGroupId.valueOf(storedUuid);
    }

    UUID groupUuid;
    if (ratisGroupCounter.size() >= omRatisGroupCount) {
      groupUuid = ratisGroupCounter.entrySet().stream()
          .min(Comparator.comparingInt(Map.Entry::getValue))
          .map(Map.Entry::getKey)
          .get();
    } else {
      String groupIdStr = getBucketId(bucketName, omRatisGroupCount);
      LOG.trace("Generate bucket name {}, group number {}", bucketName, groupIdStr);
      groupUuid = toUuid(groupIdStr);
    }

    storeTable(volumeName, bucketName, groupUuid);

    return RaftGroupId.valueOf(groupUuid);
  }

  public Map<String, UUID> getBucketRatisGroups() {
    return bucketRatisGroups;
  }

  private void storeTable(String volumeName, String bucketName, UUID groupId) {
    String key = metadataManager.getBucketKey(volumeName, bucketName);
    try {
      // OmBucketInfo is immutable and the bucket table is fully cached: update both the cache and the DB,
      // otherwise reads (served from the cache) and later double-buffer flushes would not see the group.
      OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(key);
      if (omBucketInfo == null) {
        // the request fails later with BUCKET_NOT_FOUND
        LOG.debug("Bucket {} not found, raft group {} is not persisted", key, groupId);
        return;
      }
      bucketRatisGroups.put(key, groupId);
      ratisGroupCounter.compute(groupId, (k, v) -> v == null ? 1 : v + 1);
      OmBucketInfo updated = omBucketInfo.toBuilder().setRaftGroup(groupId).build();
      metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(key),
          CacheValue.get(updated.getUpdateID(), updated));
      metadataManager.getBucketTable().put(key, updated);
    } catch (IOException e) {
      LOG.error("Couldn't find bucket v={}, b={}", volumeName, bucketName, e);
      throw new RuntimeException(e);
    }
  }

  private static String getBucketId(String ratisGroupPlainStr, int groupCount) {
    return String.valueOf(Math.abs(ratisGroupPlainStr.hashCode() % groupCount));
  }

  private static UUID toUuid(String groupId) {
    return UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
