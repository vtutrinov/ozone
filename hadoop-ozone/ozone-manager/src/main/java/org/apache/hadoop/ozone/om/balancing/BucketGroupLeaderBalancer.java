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

package org.apache.hadoop.ozone.om.balancing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for balancing leaders of multi raft bucket groups.
 */
public final class BucketGroupLeaderBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(BucketGroupLeaderBalancer.class);

  private BucketGroupLeaderBalancer() {
  }

  public static List<LeaderChangingGroupInfo> getLeaderChangingInfo(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo,
                                                                    Set<RaftPeerId> peerIdSet) {
    if (isAlreadyBalanced(groupsWithLeaderInfo, peerIdSet)) {
      return Collections.emptyList();
    }

    List<LeaderChangingGroupInfo> leaderChangingGroupInfoList = new ArrayList<>();
    int counter = 0;
    RaftPeerId[] peerIdSetArray = new RaftPeerId[peerIdSet.size()];
    peerIdSet.toArray(peerIdSetArray);
    Arrays.sort(peerIdSetArray, Comparator.comparing(RaftPeerId::toString));

    for (Map.Entry<RaftGroupId, RaftPeerId> entry : groupsWithLeaderInfo.entrySet()) {
      RaftPeerId newLeader = peerIdSetArray[counter % peerIdSetArray.length];
      if (!newLeader.equals(entry.getValue())) {
        leaderChangingGroupInfoList.add(
            new LeaderChangingGroupInfo(
                entry.getKey(),
                entry.getValue(),
                newLeader
            )
        );
      }
      counter++;
    }

    return leaderChangingGroupInfoList;
  }

  /**
   * @param groupsWithLeaderInfo leader of each bucket raft group
   * @param peerIdSet all OM peers; a peer leading no group counts as 0
   * @return true if the leader counts differ by at most 1 and do not increase in peer id order
   */
  public static boolean isAlreadyBalanced(Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo,
      Set<RaftPeerId> peerIdSet) {
    Map<RaftPeerId, Long> raftGroupLeadersCountMap = new HashMap<>();
    peerIdSet.forEach(peer -> raftGroupLeadersCountMap.put(peer, 0L));
    groupsWithLeaderInfo.values().forEach(leader -> raftGroupLeadersCountMap.merge(leader, 1L, Long::sum));

    long maxValue = raftGroupLeadersCountMap.values().stream().max(Long::compareTo).orElse(0L);
    long minValue = raftGroupLeadersCountMap.values().stream().min(Long::compareTo).orElse(0L);
    if (!isNodesCountDecrease(raftGroupLeadersCountMap)) {
      return false;
    }
    if (maxValue - minValue <= 1) {
      LOG.trace("Bucket groups is already balanced");
      return true;
    } else {
      LOG.trace("Bucket groups is not balanced. Max value: {}. Min value: {}.", maxValue, minValue);
      return false;
    }
  }

  /**
   * Om1 should has max number. Om2 should not be greater than Om1. Om3 should not be greater than Om2.
   * @param raftGroupLeadersCountMap  peers to groups count map
   * @return is nodes count decrease
   */
  private static boolean isNodesCountDecrease(Map<RaftPeerId, Long> raftGroupLeadersCountMap) {
    List<RaftPeerId> keys =
        raftGroupLeadersCountMap.keySet().stream()
                .sorted(Comparator.comparing(RaftPeerId::toString))
                .collect(Collectors.toList());
    Long prevVal = null;
    for (RaftPeerId raftPeerId : keys) {
      Long value = raftGroupLeadersCountMap.get(raftPeerId);
      if (prevVal != null && value > prevVal) {
        return false;
      }
      prevVal = value;
    }
    return true;
  }
}
