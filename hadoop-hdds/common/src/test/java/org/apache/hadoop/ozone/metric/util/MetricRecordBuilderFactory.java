/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.metric.util;

import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.filter.GlobFilter;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;

/**
 * Factory for metrics record builder.
 */
public final class MetricRecordBuilderFactory {

  /**
   * Private constructor for util class.
   */
  private MetricRecordBuilderFactory() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create metrics record builder for test purposes.
   * @return metrics record builder
   */
  public static MetricsRecordBuilderImpl getMetricsRecordBuilder() {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    MetricsInfo metricsInfo = new MetricsInfo() {
      @Override
      public String name() {
        return "name";
      }

      @Override
      public String description() {
        return "desc";
      }
    };
    MetricsFilter metricsFilter = new GlobFilter();
    return new MetricsRecordBuilderImpl(collector, metricsInfo, metricsFilter, metricsFilter, true);
  }
}
