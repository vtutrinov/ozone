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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.ozone.metrics.OzoneSampleQuantiles;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SampleQuantiles.
 */
class SampleQuantilesTest {

  private static final Quantile[] QUANTILES = {new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001)};

  @Test
  void testSnapshotEmptySnapshotReturnEmptyMap() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(0, snapshot.size());
  }

  @Test
  void testSnapshotHasCorrectQuantilesSize() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(5, snapshot.size());
  }

  @Test
  void testSnapshotHasCorrectQuantilesNames() {
    Set<Double> expectedQuantiles = Stream.of(
            0.5,
            0.75,
            0.9,
            0.95,
            0.99
    ).collect(Collectors.toSet());
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    Set<Double> actualQuantiles = snapshot.keySet().stream().map(aLong -> aLong.quantile).collect(Collectors.toSet());
    assertEquals(expectedQuantiles, actualQuantiles);
  }

  @Test
  void testClearDontRemoveQuantilesFromSnapshot() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    quantiles.clear();

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(5, snapshot.size());
  }

  @Test
  void testClearResetQuantilesInSnapshot() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    quantiles.clear();

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    snapshot.forEach((key, value) -> assertEquals(0, value));
  }

  @Test
  void testGetCount() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    long count = quantiles.getCount();
    assertEquals(10, count);
  }

  @Test
  void testGetStateAndClearGetCorrectState() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);
    Map<Quantile, Long> snapshot = quantiles.snapshot();
    Pair<Long, Map<Quantile, Long>> stateAndClear = quantiles.getStateAndClear();
    assertEquals(10, stateAndClear.getKey());
    assertEquals(snapshot, stateAndClear.getValue());
  }

  @Test
  void testGetStateAndClearResetState() {
    OzoneSampleQuantiles quantiles = new OzoneSampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    quantiles.getStateAndClear();
    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertTrue(snapshot.isEmpty());
  }

  private static void insertTenElements(OzoneSampleQuantiles metric) {
    for (int i = 0; i < 10; i++) {
      metric.insert(i);
    }
  }
}
