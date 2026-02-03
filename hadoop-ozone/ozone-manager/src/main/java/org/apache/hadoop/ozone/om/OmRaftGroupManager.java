package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRaftGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRaftGroupManager.class);

  private final int omRaftGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;

  private final Map<String, UUID> bucketRaftGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> bucketsPerRaftGroupCounter = new ConcurrentHashMap<>();

  public OmRaftGroupManager(
      OzoneConfiguration configuration,
      boolean multiRaftEnabled,
      String omServiceId,
      OMMetadataManager metadataManager
  ) {
    omRaftGroupCount = configuration.getInt(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServiceId = omServiceId;
    this.metadataManager = metadataManager;
  }

  public void reset() {
    bucketRaftGroups.clear();
    bucketsPerRaftGroupCounter.clear();
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
          bucketRaftGroups.put(key, raftGroup);
          bucketsPerRaftGroupCounter.compute(raftGroup, (k, v) -> v == null ? 1 : v + 1);
        }
      }
    }
  }

  public synchronized RaftGroupId raftGroupName(String volumeName, String bucketName, HddsProtos.UUID raftGroupId) {
    UUID raftGroupUUID = new UUID(raftGroupId.getMostSigBits(), raftGroupId.getLeastSigBits());
    RaftGroupId raftGroupIdToHandleRequest = RaftGroupId.valueOf(raftGroupUUID);
    storeTable(volumeName, bucketName, raftGroupUUID);
    return raftGroupIdToHandleRequest;
  }

  public synchronized RaftGroupId raftGroupName(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return RaftGroupId.valueOf(toUuid(omServiceId));
    }

    String key = metadataManager.getBucketKey(volumeName, bucketName);
    UUID storedUuid = bucketRaftGroups.get(key);
    if (storedUuid != null && bucketsPerRaftGroupCounter.containsKey(storedUuid)) {
      LOG.trace("Return stored uuid {}", storedUuid);
      return RaftGroupId.valueOf(storedUuid);
    }

    while (bucketsPerRaftGroupCounter.size() < omRaftGroupCount) {
      try {
        LOG.info(
                "Waiting for group initiating {}-{}. {}", bucketsPerRaftGroupCounter.size(), omRaftGroupCount, bucketsPerRaftGroupCounter
        );
        wait(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    UUID groupUuid = bucketsPerRaftGroupCounter.entrySet().stream()
            .min(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();

    storeTable(volumeName, bucketName, groupUuid);

    return RaftGroupId.valueOf(groupUuid);
  }

  public Map<String, UUID> getBucketRaftGroups() {
    return bucketRaftGroups;
  }

  public int getOmRaftGroupCount() {
    return omRaftGroupCount;
  }

  public static List<RaftGroupId> generateRaftGroups(long currentTerm, int count) {
    List<RaftGroupId> result = new ArrayList<>(count);
    long startFrom = currentTerm * 100;
    for (long i = startFrom; i < startFrom + count; i++) {
      UUID raftGroupIdUUID = OmRaftGroupManager.toUuid(String.valueOf(i));
      RaftGroupId groupId = RaftGroupId.valueOf(raftGroupIdUUID);
      result.add(groupId);
    }
    return result;
  }

  private void storeTable(String volumeName, String bucketName, UUID groupId) {
    String key = metadataManager.getBucketKey(volumeName, bucketName);
    bucketRaftGroups.put(key, groupId);
    bucketsPerRaftGroupCounter.compute(groupId, (k, v) -> v == null ? 1 : v + 1);

    try {
      OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(key);
      omBucketInfo.setRaftGroup(groupId);
      metadataManager.getBucketTable().put(key, omBucketInfo);
    } catch (IOException e) {
      LOG.error("Couldn't find bucket v={}, b={}", volumeName, bucketName, e);
      throw new RuntimeException(e);
    }
  }

  public static UUID toUuid(String groupId) {
    return UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public RaftGroupId addGroupIdToRaftGroupCounter(UUID groupUuid) {
    bucketsPerRaftGroupCounter.put(groupUuid, 0);
    return RaftGroupId.valueOf(groupUuid);
  }

  public void addGroupIdListToRaftGroupCounter(List<UUID> groupUuid) {
    bucketsPerRaftGroupCounter.clear();
    groupUuid.forEach(it -> bucketsPerRaftGroupCounter.put(it, 0));
  }
}
