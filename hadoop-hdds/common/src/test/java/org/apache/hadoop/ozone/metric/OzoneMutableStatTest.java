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
import org.apache.hadoop.metrics2.util.SampleStat;
import org.apache.hadoop.ozone.metric.util.MetricsRecordBuilderImpl;
import org.apache.hadoop.ozone.metrics.OzoneMutableStat;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.metric.util.MetricRecordBuilderFactory.getMetricsRecordBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for OzoneMutableStat.
 */
class OzoneMutableStatTest {

  @Test
  void testOzoneMutableStatHasEmptyMetricsAfterCreation() {
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(0, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElements() {
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNamesAfterInsertElements() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameNumTest_sample_name",
            "Test_nameAvgTest_value_name"
    ).collect(Collectors.toSet());

    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertElements() {
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = metrics.get(0).value();
    assertEquals(10L, numberOfSamples);

    Number averageTimeMetricValue = metrics.get(1).value();
    assertEquals(5.5, averageTimeMetricValue);
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertWithSumElements() {
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNameAfterInsertWithSumElements() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameNumTest_sample_name",
            "Test_nameAvgTest_value_name"
    ).collect(Collectors.toSet());
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertWithSumElements() {
    OzoneMutableStat metric = createMutableStat();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = metrics.get(0).value();
    assertEquals(15L, numberOfSamples);

    Number averageTime = metrics.get(1).value();
    assertEquals(20.0, averageTime);
  }

  @Test
  void testOzoneMutableStatChangedWhenElementsAdded() {
    OzoneMutableStat metric = createMutableStat();

    insertTenElements(metric);
    assertTrue(metric.changed());
  }

  @Test
  void testOzoneMutableStatNotChangedWhenNoElementsAdded() {
    OzoneMutableStat metric = createMutableStat();

    assertFalse(metric.changed());
  }

  @Test
  void testGetLastStatWithAddMethod() {
    OzoneMutableStat metric = createMutableStat();

    insertTenElements(metric);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(1, sampleStat.min());
    assertEquals(10, sampleStat.max());
    assertEquals(5.5, sampleStat.mean());
    assertEquals(10, sampleStat.numSamples());
  }

  @Test
  void testGetLastStatWithAddSumMethod() {
    OzoneMutableStat metric = createMutableStat();

    insertElementsWithSum(metric);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(Float.MAX_VALUE, sampleStat.min());
    assertEquals(Float.MIN_VALUE, sampleStat.max());
    assertEquals(20.0, sampleStat.mean());
    assertEquals(15, sampleStat.numSamples());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElements1() {
    OzoneMutableStat metric = createMutableStat();
    metric.setUpdateTimeStamp(true);
    assertEquals(0, metric.getSnapshotTimeStamp());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElements2() {
    OzoneMutableStat metric = createMutableStat();
    metric.setUpdateTimeStamp(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);

    assertTrue(metric.getSnapshotTimeStamp() > 0);
  }

  private static void insertTenElements(OzoneMutableStat metric) {
    for (int i = 1; i <= 10; i++) {
      metric.add(i);
    }
  }

  private void insertElementsWithSum(OzoneMutableStat metric) {
    int numberOfSamples = 5;
    int samplesSum = 100;

    metric.add(numberOfSamples, samplesSum);
    metric.add(numberOfSamples, samplesSum);
    metric.add(numberOfSamples, samplesSum);
  }

  private static OzoneMutableStat createMutableStat() {
    return new OzoneMutableStat(
        "Test_name",
        "Test_description",
        "Test_sample_name",
        "Test_value_name");
  }
}
