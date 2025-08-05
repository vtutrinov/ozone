package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.OMInSafeModeException;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_CHECK_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_CHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED_DEFAULT;

public class SafeModeManager extends BackgroundService {

  public static final Logger LOG = LoggerFactory.getLogger(SafeModeManager.class);

  private final AtomicBoolean inSafeMode = new AtomicBoolean(true);
  private final AtomicBoolean omLeaderReady = new AtomicBoolean(false);
  private final AtomicBoolean bucketGroupsReady = new AtomicBoolean(false);

  private final boolean safeModeEnabled;
  private final int bucketRaftGroupsExpectedCount;
  private final long safeModeCheckInterval;
  private final AtomicInteger bucketGroupsReadyCount = new AtomicInteger(0);


  public SafeModeManager(OzoneConfiguration configuration) {
    super("SafeModeManager",
        configuration.getTimeDuration(OZONE_OM_SAFE_MODE_CHECK_INTERVAL,
            OZONE_OM_SAFE_MODE_CHECK_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS, 1, 0, "OMSafeModeManager-");
    this.safeModeEnabled = configuration.getBoolean(OZONE_OM_SAFE_MODE_ENABLED, OZONE_OM_SAFE_MODE_ENABLED_DEFAULT);
    this.bucketRaftGroupsExpectedCount = configuration.getInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
    this.safeModeCheckInterval = configuration.getTimeDuration(OZONE_OM_SAFE_MODE_CHECK_INTERVAL,
        OZONE_OM_SAFE_MODE_CHECK_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
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

  public void onBucketGroupReady() {
    if (safeModeEnabled) {
      int count = bucketGroupsReadyCount.incrementAndGet();
      LOG.info("OM bucket group ready count: {}/{}", count, bucketRaftGroupsExpectedCount);
      if (count >= bucketRaftGroupsExpectedCount) {
        bucketGroupsReady.set(true);
        tryLeaveSafeMode();
      }
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

  public static class SafeModeCheckTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      return null;
    }

    @Override
    public int getPriority() {
      return 0;
    }

  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    if (safeModeEnabled) {
      queue.add(new SafeModeCheckTask());
    }
    return queue;
  }
}
