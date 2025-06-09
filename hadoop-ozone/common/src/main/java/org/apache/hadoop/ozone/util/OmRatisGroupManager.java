package org.apache.hadoop.ozone.util;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRatisGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRatisGroupManager.class);

  //TODO SDPOZN-1709 Maybe it needs change map to cache
  private final Map<String, RaftGroupId> groupIdMap = new ConcurrentHashMap<>();

  private final int omRatisGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServceId;

  public OmRatisGroupManager(OzoneConfiguration configuration, boolean multiRaftEnabled, String omServceId) {
    omRatisGroupCount = configuration.getInt(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServceId = omServceId;
  }

  public RaftGroupId ratisGroupName(String bucketName) {
    String groupId = (bucketName != null && multiRaftEnabled) ? getBucketId(bucketName, omRatisGroupCount) : omServceId;
    LOG.trace("Generate bucket name {}, group number {}", bucketName, groupId);
    return groupIdMap.computeIfAbsent(groupId, OmRatisGroupManager::toRaftGroupId);
  }

  private static String getBucketId(String raftGroupPlainStr, int groupCount) {
    return String.valueOf(Math.abs(raftGroupPlainStr.hashCode() % groupCount));
  }

  private static RaftGroupId toRaftGroupId(String groupId) {
    UUID uuid = UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    LOG.trace("Generate bucket group number {}, generated uuid {}", groupId, uuid);
    return RaftGroupId.valueOf(uuid);
  }
}
