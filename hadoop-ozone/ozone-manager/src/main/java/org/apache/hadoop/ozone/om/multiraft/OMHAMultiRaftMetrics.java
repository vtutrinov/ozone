package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;

@Metrics(
    about = "Ozone Manager HA Multi-Raft Metrics",
    context = "ozone")
public class OMHAMultiRaftMetrics implements MetricsSource  {

  private final OzoneManager ozoneManager;

  private static final String SOURCE_NAME = OMHAMultiRaftMetrics.class.getSimpleName();

  public OMHAMultiRaftMetrics(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  public static OMHAMultiRaftMetrics create(OzoneManager ozoneManager) {
    OMHAMultiRaftMetrics omhaMultiRaftMetrics = new OMHAMultiRaftMetrics(ozoneManager);
    return OzoneMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", omhaMultiRaftMetrics);
  }

  public static void unRegister() {
    OzoneMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("RaftGroupsCount", "OM Raft groups Count"), ozoneManager.getOmRaftGroups().size())
        .endRecord();
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("RaftGroupsExpectedCount", "OM Raft Groups Expected Count"),
            ozoneManager.getConfiguration().getInt(OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
                OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT) + 1)
        .endRecord();
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("OMInSafeMode", "Ozone Manager is in safe mode"),
            ozoneManager.getSafeModeManager().isInSafeMode() ? 1 : 0)
        .endRecord();
  }
}
