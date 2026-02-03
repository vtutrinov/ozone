package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OmStatusChecker {

  private static final Logger LOG = LoggerFactory.getLogger(OmStatusChecker.class);

  private final OzoneManager ozoneManager;

  public OmStatusChecker(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  public boolean areAllOMsOnline(RaftGroupId raftGroupId) {
    if (!ozoneManager.isRatisEnabled()) {
      LOG.info("Ratis is not enabled, skipping OM status check.");
      return true;
    }
    try {
      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftServer.Division division = omRatisServer.getServer().getDivision(raftGroupId);
      List<RaftProtos.ServerRpcProto> followerInfoList = division.getInfo().getRoleInfoProto().getLeaderInfo()
          .getFollowerInfoList();
      int configuredPeers = ozoneManager.getPeerNodes().size();
      return followerInfoList.size() == configuredPeers;
    } catch (IOException ex) {
      LOG.error("Error occurred on getting OM services list: ", ex);
      return false;
    }
  }

}
