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


package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StateMachine for group of buckets. It is
 * responsible for applying Ratis committed bucket write transactions to
 * {@link OzoneManager}.
 * <p>
 * A bucket raft group shares the OM DB with the OM raft group: it applies requests the same way
 * as {@link OzoneManagerStateMachine}, but persists its applied index under its own
 * TransactionInfo key and does not drive the OM-level leader/peer state.
 */
public class BucketStateMachine extends OzoneManagerStateMachine {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStateMachine.class);

  public BucketStateMachine(RaftGroupId raftGroupId, OzoneManager om) throws IOException {
    super(om, raftGroupId, "-" + raftGroupId + "-",
        TracingUtil.isTracingEnabled(om.getConfiguration()));
  }

  @Override
  protected String getTransactionInfoKey() {
    return TRANSACTION_INFO_KEY + getRaftGroupId();
  }

  @Override
  protected TransactionInfo getGroupTransactionInfo() {
    return getOzoneManager().getTransactionInfo(getRaftGroupId());
  }

  @Override
  protected void setGroupTransactionInfo(TransactionInfo info) {
    getOzoneManager().setTransactionInfo(getRaftGroupId(), info);
  }

  @Override
  protected RaftServer.Division getServerDivision() {
    return getOzoneManager().getOmRatisServer().getServerDivision(getRaftGroupId());
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
    LOG.trace("Change leader in group {}. New leader {}", groupMemberId, newLeaderId);
    // Initialize OMHAMetrics
    getOzoneManager().omHAMetricsInit(newLeaderId.toString());
  }

  @Override
  public void notifyLeaderReady() {
    // OM-level leader state is driven by the OM raft group only.
    LOG.trace("Leader ready for {} - {}. OM leader: {}",
        getRaftGroupId(),
        getOzoneManager().getOmRatisServer().getLeaderId(getRaftGroupId()),
        getOzoneManager().getOmRatisServer().getLeaderId(getOzoneManager().omRatisGroupName()));
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    // OM-level leader state is driven by the OM raft group only.
  }

  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
    // OM peer list is driven by the OM raft group only.
    LOG.trace("{}: configuration changed at ({}, {})", getRaftGroupId(), term, index);
  }

}
