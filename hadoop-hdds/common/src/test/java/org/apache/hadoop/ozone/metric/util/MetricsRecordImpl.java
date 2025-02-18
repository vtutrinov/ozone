package org.apache.hadoop.ozone.metric.util;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.util.Contracts;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;

class MetricsRecordImpl extends AbstractMetricsRecord {

  protected static final String DEFAULT_CONTEXT = "default";
  private final long timestamp;
  private final MetricsInfo info;
  private final List<MetricsTag> tags;
  private final Iterable<AbstractMetric> metrics;
  MetricsRecordImpl(MetricsInfo info, long timestamp, List<MetricsTag> tags, Iterable<AbstractMetric> metrics) {
    this.timestamp = Contracts.checkArg(timestamp, timestamp > 0L, "timestamp");
    this.info = (MetricsInfo) Preconditions.checkNotNull(info, "info");
    this.tags = (List) Preconditions.checkNotNull(tags, "tags");
    this.metrics = (Iterable) Preconditions.checkNotNull(metrics, "metrics");
  }
  public long timestamp() {
    return this.timestamp;
  }
  public String name() {
    return this.info.name();
  }
  MetricsInfo info() {
    return this.info;
  }
  public String description() {
    return this.info.description();
  }
  public String context() {
    Iterator var1 = this.tags.iterator();
    MetricsTag t;
    do {
      if (!var1.hasNext()) {
        return "default";
      }
      t = (MetricsTag) var1.next();
    } while (t.info() != MsInfo.Context);
    return t.value();
  }
  public List<MetricsTag> tags() {
    return this.tags;
  }
  public Iterable<AbstractMetric> metrics() {
    return this.metrics;
  }
}
