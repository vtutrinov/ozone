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

package org.apache.hadoop.ozone.metric;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.ozone.metric.util.MetricsRecordBuilderImpl;
import org.apache.hadoop.ozone.metrics.OzoneMutableQuantiles;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.metric.util.MetricRecordBuilderFactory.getMetricsRecordBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for MutableQuantiles.
 */
class MutableQuantilesTest {

  @Test
  void testIsChangedAfterTaskStart() {
    OzoneMutableQuantiles quantiles = getMutableQuantiles();
    insertTenElements(quantiles);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(quantiles::changed);
    quantiles.stop();
  }

  @Test
  void testSnapshotHasCorrectQuantilesSize() {
    OzoneMutableQuantiles quantiles = getMutableQuantiles();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(quantiles);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
      quantiles.snapshot(metricsRecordBuilder);

      List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

      assertEquals(6L, metrics.size());
    });
    quantiles.stop();
  }

  @Test
  void testSnapshotHasCorrectMetricNames() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameNumTest_sample_name",
            "Test_name50thPercentileTest_value_name",
            "Test_name75thPercentileTest_value_name",
            "Test_name90thPercentileTest_value_name",
            "Test_name95thPercentileTest_value_name",
            "Test_name99thPercentileTest_value_name"
    ).collect(Collectors.toSet());

    OzoneMutableQuantiles quantiles = getMutableQuantiles();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(quantiles);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
      quantiles.snapshot(metricsRecordBuilder);

      List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
      Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
      assertEquals(expectedMetricNames, actualMetricNames);
    });
    quantiles.stop();
  }

  @Test
  void testMetrics() {
    OzoneMutableQuantiles quantiles = getMutableQuantiles();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(quantiles);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
      quantiles.snapshot(metricsRecordBuilder);

      List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
      assertFalse(metrics.isEmpty());

      Number sampleNumbers = getSampleNumbers(metrics);
      assertEquals(10L, sampleNumbers);

      Number metrics50thPercentileValue = get50thPercentile(metrics);
      assertEquals(4L, metrics50thPercentileValue);

      Number metrics75thPercentileValue = get75thPercentile(metrics);
      assertEquals(6L, metrics75thPercentileValue);

      Number metrics90thPercentileValue = get90thPercentile(metrics);
      assertEquals(8L, metrics90thPercentileValue);

      Number metrics95thPercentileValue = get95thPercentile(metrics);
      assertEquals(8L, metrics95thPercentileValue);

      Number metrics99thPercentileValue = get99thPercentile(metrics);
      assertEquals(8L, metrics99thPercentileValue);
    });
    quantiles.stop();
  }

  private static OzoneMutableQuantiles getMutableQuantiles() {
    return new OzoneMutableQuantiles("Test_name",
        "Test_description",
        "Test_sample_name",
        "Test_value_name",
        1);
  }

  private static void insertTenElements(OzoneMutableQuantiles metric) {
    for (int i = 0; i < 10; i++) {
      metric.add(i);
    }
  }

  private Number getSampleNumbers(List<AbstractMetric> metrics) {
    return metrics.get(0).value();
  }

  private Number get50thPercentile(List<AbstractMetric> metrics) {
    return metrics.get(1).value();
  }

  private Number get75thPercentile(List<AbstractMetric> metrics) {
    return metrics.get(2).value();
  }

  private Number get90thPercentile(List<AbstractMetric> metrics) {
    return metrics.get(3).value();
  }

  private Number get95thPercentile(List<AbstractMetric> metrics) {
    return metrics.get(4).value();
  }

  private Number get99thPercentile(List<AbstractMetric> metrics) {
    return metrics.get(5).value();
  }
}
