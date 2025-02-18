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

package org.apache.hadoop.ozone.metric.util;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.util.Contracts;

class MetricsRecordImpl extends AbstractMetricsRecord {

  protected static final String DEFAULT_CONTEXT = "default";
  private final long timestamp;
  private final MetricsInfo info;
  private final List<MetricsTag> tags;
  private final Iterable<AbstractMetric> metrics;

  MetricsRecordImpl(MetricsInfo info, long timestamp, List<MetricsTag> tags, Iterable<AbstractMetric> metrics) {
    this.timestamp = Contracts.checkArg(timestamp, timestamp > 0L, "timestamp");
    this.info = (MetricsInfo) Objects.requireNonNull(info, "info");
    this.tags = (List) Objects.requireNonNull(tags, "tags");
    this.metrics = (Iterable) Objects.requireNonNull(metrics, "metrics");
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
