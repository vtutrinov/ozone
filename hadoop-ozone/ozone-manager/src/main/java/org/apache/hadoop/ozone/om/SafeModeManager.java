package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.OMInSafeModeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED_DEFAULT;

public class SafeModeManager {

  public static final Logger LOG = LoggerFactory.getLogger(SafeModeManager.class);

  private final AtomicBoolean inSafeMode = new AtomicBoolean(true);
  private final AtomicBoolean omLeaderReady = new AtomicBoolean(false);
  private final AtomicBoolean bucketGroupsReady = new AtomicBoolean(false);

  private final boolean safeModeEnabled;


  public SafeModeManager(OzoneConfiguration configuration) {
    this.safeModeEnabled = configuration.getBoolean(OZONE_OM_SAFE_MODE_ENABLED, OZONE_OM_SAFE_MODE_ENABLED_DEFAULT);
    if (!safeModeEnabled) {
      inSafeMode.set(false);
      LOG.info("OM safe mode is disabled by configuration");
    }
  }

  public void checkSafeMode() throws OMInSafeModeException {
    if (safeModeEnabled && inSafeMode.get()) {
      throw new OMInSafeModeException("OM is in safe mode. Please wait until it exits safe mode.");
    }
  }

  public boolean isInSafeMode() {
    return safeModeEnabled && inSafeMode.get();
  }

  public synchronized void onLeaderElected() {
    if (safeModeEnabled) {
      LOG.info("OM leader elected.");
      omLeaderReady.set(true);
      tryLeaveSafeMode();
    }
  }

  public synchronized void onLeadershipLost() {
    if (safeModeEnabled) {
      LOG.info("OM leadership lost.");
      omLeaderReady.set(false);
      inSafeMode.set(true); // Re-enter safe mode if leadership is lost
    }
  }

  public synchronized void onBucketRaftGroupsReady() {
    if (safeModeEnabled) {
      LOG.info("OM bucket groups are ready.");
      bucketGroupsReady.set(true);
      tryLeaveSafeMode();
    }
  }

  public void tryLeaveSafeMode() {
    if (safeModeEnabled && omLeaderReady.get() && bucketGroupsReady.get()) {
      inSafeMode.compareAndSet(true, false);
      LOG.info("OM is leaving safe mode.");
    } else {
      LOG.info("OM cannot exit safe mode yet. Leader ready: {}, Bucket groups ready: {}",
          omLeaderReady.get(), bucketGroupsReady.get());
    }
  }

}
