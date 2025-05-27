package org.apache.hadoop.ozone.util;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.protocol.RaftGroupId;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Raft group id generator with cache.
 */
public final class OzoneRaftGroupIdGenerator {

  private static final org.slf4j.Logger LOG =
          org.slf4j.LoggerFactory.getLogger(OzoneRaftGroupIdGenerator.class);
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  //TODO SDPOZN-1709 Maybe it needs change map to cache
  private static final Map<String, RaftGroupId> GROUP_ID_MAP = new ConcurrentHashMap<>();

  private OzoneRaftGroupIdGenerator() {
  }

  public static RaftGroupId generateLimitedRaftGroupId(String raftGroupPlainStr) {
    int maxRaftGroups = CONF.getInt(
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
    int groupNumber = Math.abs(raftGroupPlainStr.hashCode() % maxRaftGroups);
    String stringGroupNumber = String.valueOf(groupNumber);
    return getId(stringGroupNumber);
  }

  public static RaftGroupId generateRaftGroupId(String raftGroupPlainStr) {
    return getId(raftGroupPlainStr);
  }

  private static RaftGroupId getId(String groupId) {
    return GROUP_ID_MAP.computeIfAbsent(groupId, (k) -> {
      UUID uuid = UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      LOG.trace("Generate bucket group number {}, generated uuid {}", groupId, uuid);
      return RaftGroupId.valueOf(uuid);
    });
  }

}
