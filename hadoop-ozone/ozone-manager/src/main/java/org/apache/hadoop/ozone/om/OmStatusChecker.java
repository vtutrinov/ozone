package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
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

  public boolean areAllOMsOnline() {
    if (!ozoneManager.isRatisEnabled()) {
      LOG.info("Ratis is not enabled, skipping OM status check.");
      return true;
    }
    int configuredPeers = ozoneManager.getPeerNodes().size();
    int expectedOMCount = configuredPeers + 1;
    try {
      List<ServiceInfo> onlineOMs = ozoneManager.getServiceList();
      return onlineOMs.size() == expectedOMCount;
    } catch (IOException ex) {
      LOG.error("Error occurred on getting OM services list: ", ex);
      return false;
    }
  }

}
