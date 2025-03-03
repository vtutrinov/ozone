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
import org.apache.hadoop.ozone.metrics.OzoneMutableRate;
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
 * Tests for OzoneMutableRate.
 */
class OzoneMutableRateTest {

  @Test
  void testOzoneMutableStatHasEmptyMetricsAfterCreationExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(0, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElementsExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(8, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNamesAfterInsertElementsExtended() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameAvgTime",
            "Test_nameStdevTime",
            "Test_nameMinTime",
            "Test_nameIMaxTime",
            "Test_nameNumOps",
            "Test_nameIMinTime",
            "Test_nameMaxTime",
            "Test_nameINumOps"
    ).collect(Collectors.toSet());

    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertElementsExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);

    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = getNumberOfSamples(metrics);
    assertEquals(10L, numberOfSamples);

    Number averageTimeMetric = getAverageTime(metrics);
    assertEquals(5.5, averageTimeMetric);

    Number intervalMinTimeMetric = getIntervalMinTimeMetric(metrics);
    assertEquals(1.0, intervalMinTimeMetric);

    Number intervalMaxTimeMetric = getIntervalMaxTimeMetric(metrics);
    assertEquals(10.0, intervalMaxTimeMetric);

    Number minTimeMetric = getMinTimeMetric(metrics);
    assertEquals(1.0, minTimeMetric);

    Number maxTimeMetric = getMaxTimeMetric(metrics);
    assertEquals(10.0, maxTimeMetric);

    Number intervalNumberMetric = getIntervalNumberMetricValue(metrics);
    assertEquals(10L, intervalNumberMetric);
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertWithSumElementsExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(8, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNamesAfterInsertWithSumElementsExtended() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameAvgTime",
            "Test_nameStdevTime",
            "Test_nameMinTime",
            "Test_nameIMaxTime",
            "Test_nameNumOps",
            "Test_nameIMinTime",
            "Test_nameMaxTime",
            "Test_nameINumOps"
    ).collect(Collectors.toSet());

    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);

    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertWithSumElementsExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = getNumberOfSamples(metrics);
    assertEquals(15L, numberOfSamples);

    Number averageTime = getAverageTime(metrics);
    assertEquals(20.0, averageTime);

    float intervalMinTimeMetricValue = Float.parseFloat(getIntervalMinTimeMetric(metrics).toString());
    assertEquals(Float.MAX_VALUE, intervalMinTimeMetricValue);

    float intervalMaxTimeMetricValue = Float.parseFloat(getIntervalMaxTimeMetric(metrics).toString());
    assertEquals(Float.MIN_VALUE, intervalMaxTimeMetricValue);

    float minTimeMetricValue = Float.parseFloat(getMinTimeMetric(metrics).toString());
    assertEquals(Float.MAX_VALUE, minTimeMetricValue);

    float maxTimeMetricValue = Float.parseFloat(getMaxTimeMetric(metrics).toString());
    assertEquals(Float.MIN_VALUE, maxTimeMetricValue);

    Number intervalNumberMetricValue = getIntervalNumberMetricValue(metrics);
    assertEquals(15L, intervalNumberMetricValue);
  }

  @Test
  void testOzoneMutableStatHasEmptyMetricsAfterCreationNotExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(0, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElementsNotExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNamesAfterInsertElementsNotExtended() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameAvgTime",
            "Test_nameNumOps"
    ).collect(Collectors.toSet());

    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertElementsNotExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = getNumberOfSamples(metrics);
    assertEquals(10L, numberOfSamples);

    Number averageTimeMetricValue = getAverageTime(metrics);
    assertEquals(5.5, averageTimeMetricValue);
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertWithSumElementsNotExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsNamesAfterInsertWithSumElementsNotExtended() {
    Set<String> expectedMetricNames = Stream.of(
            "Test_nameAvgTime",
            "Test_nameNumOps"
    ).collect(Collectors.toSet());

    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    Set<String> actualMetricNames = metrics.stream().map(AbstractMetric::name).collect(Collectors.toSet());
    assertEquals(expectedMetricNames, actualMetricNames);
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertWithSumElementsNotExtended() {
    OzoneMutableRate metric = createMutableRate();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertElementsWithSum(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    Number numberOfSamples = getNumberOfSamples(metrics);
    assertEquals(15L, numberOfSamples);

    Number averageTime = getAverageTime(metrics);
    assertEquals(20.0, averageTime);
  }

  @Test
  void testOzoneMutableStatChangedWhenElementsAdded() {
    OzoneMutableRate metric = createMutableRate();

    insertTenElements(metric);
    assertTrue(metric.changed());
  }

  @Test
  void testOzoneMutableStatNotChangedWhenNoElementsAdded() {
    OzoneMutableRate metric = createMutableRate();

    assertFalse(metric.changed());
  }

  @Test
  void testGetLastStatWithAddMethod() {
    OzoneMutableRate metric = createMutableRate();

    insertTenElements(metric);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(1, sampleStat.min());
    assertEquals(10, sampleStat.max());
    assertEquals(5.5, sampleStat.mean());
    assertEquals(10, sampleStat.numSamples());
  }

  @Test
  void testGetLastStatWithAddSumMethod() {
    OzoneMutableRate metric = createMutableRate();

    insertElementsWithSum(metric);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(Float.MAX_VALUE, sampleStat.min());
    assertEquals(Float.MIN_VALUE, sampleStat.max());
    assertEquals(20.0, sampleStat.mean());
    assertEquals(15, sampleStat.numSamples());
  }

  @Test
  void testOzoneMutableRateEmptySnapshotTimeWithoutSnapshot() {
    OzoneMutableRate metric = createMutableRate();
    metric.setUpdateTimeStamp(true);
    assertEquals(0, metric.getSnapshotTimeStamp());
  }

  @Test
  void testOzoneMutableRateSnapshotTimeStampAfterInsert() {
    OzoneMutableRate metric = createMutableRate();
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

  private void insertElementsWithSum(OzoneMutableRate metric) {
    int numberOfSamples = 5;
    int samplesSum = 100;

    metric.add(numberOfSamples, samplesSum);
    metric.add(numberOfSamples, samplesSum);
    metric.add(numberOfSamples, samplesSum);
  }

  private static OzoneMutableRate createMutableRate() {
    return new OzoneMutableRate(
        "Test_name",
        "Test_description",
        false);
  }

  private Number getNumberOfSamples(List<AbstractMetric> metrics) {
    return metrics.get(0).value();
  }

  private Number getAverageTime(List<AbstractMetric> metrics) {
    return metrics.get(1).value();
  }

  private Number getIntervalMinTimeMetric(List<AbstractMetric> metrics) {
    return metrics.get(3).value();
  }

  private Number getIntervalMaxTimeMetric(List<AbstractMetric> metrics) {
    return metrics.get(4).value();
  }

  private Number getMinTimeMetric(List<AbstractMetric> metrics) {
    return metrics.get(5).value();
  }

  private Number getMaxTimeMetric(List<AbstractMetric> metrics) {
    return metrics.get(6).value();
  }

  private Number getIntervalNumberMetricValue(List<AbstractMetric> metrics) {
    return metrics.get(7).value();
  }
}
