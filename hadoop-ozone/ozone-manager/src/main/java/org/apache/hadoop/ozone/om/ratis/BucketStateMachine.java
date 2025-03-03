package org.apache.hadoop.ozone.om.ratis;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import java.io.IOException;

public class BucketStateMachine extends BaseStateMachine {

  private final String bucketName;

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  public BucketStateMachine(String bucketName) {
    this.bucketName = bucketName;
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(raftServer, raftGroupId, raftStorage);
      storage.init(raftStorage);
      LOG.info("{}: initialize {} with {}", getId(), raftGroupId, getLastAppliedTermIndex());
    });
  }
}
