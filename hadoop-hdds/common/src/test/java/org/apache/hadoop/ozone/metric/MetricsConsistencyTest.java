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
import org.apache.hadoop.ozone.metrics.OzoneMutableRate;
import org.apache.hadoop.ozone.metrics.OzoneMutableStat;
import org.apache.ozone.test.tag.Unhealthy;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.metric.util.MetricRecordBuilderFactory.getMetricsRecordBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for checking the correctness of metrics while concurrent collecting.
 */
public class MetricsConsistencyTest {

  @Test
  @Unhealthy("SDPOZN-1526")
  void testOzoneMutableRateExtendedConsistencyTest() throws InterruptedException {
    OzoneMutableRate rate = new OzoneMutableRate("Test name", "Test description", true);
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      int finalI = i;

      executor.execute(() -> {
        rate.add(finalI);
        if (finalI % 100 == 0) {
          rate.snapshot(getMetricsRecordBuilder());
        }
      });
    }

    Awaitility.await()
            .pollDelay(300, TimeUnit.MILLISECONDS)
            .atMost(5, TimeUnit.SECONDS).until(() -> {
              rate.snapshot(builder);
              return !builder.metrics().isEmpty();
            });

    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(1000, numOpsMetric.value().intValue());
  }

  @Test
  @Unhealthy("SDPOZN-1526")
  void testOzoneMutableRateNotExtendedConsistencyTest() throws InterruptedException {
    OzoneMutableRate metric = new OzoneMutableRate("Test name", "Test description", false);
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    final MetricsRecordBuilderImpl builder1 = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      int finalI = i;

      executor.execute(() -> {
        metric.add(finalI);
        if (finalI % 100 == 0) {
          metric.snapshot(builder1);
        }
      });
    }

    Awaitility.await()
            .pollDelay(300, TimeUnit.MILLISECONDS)
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> {
              metric.snapshot(builder1);
              return !builder.metrics().isEmpty();
            });
    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(1000, numOpsMetric.value().intValue());
  }

  @Test
  @Unhealthy("SDPOZN-1526")
  void testOzoneMutableStatConsistencyTest() throws InterruptedException {
    OzoneMutableStat metric = new OzoneMutableStat(
            "Test_name",
            "Test_description",
            "Test_sample_name",
            "Test_value_name");
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      int finalI = i;

      executor.execute(() -> {
        metric.add(finalI);
        if (finalI % 100 == 0) {
          metric.snapshot(getMetricsRecordBuilder());
        }
      });
    }

    Awaitility.await()
            .pollDelay(300, TimeUnit.MILLISECONDS)
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> {
                      metric.snapshot(builder);
                      return !builder.metrics().isEmpty();
                    }
        );

    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(1000, numOpsMetric.value().intValue());
    executor.shutdownNow();
  }


  @Test
  @Unhealthy("SDPOZN-1526")
  void testMutableQuantilesConsistencyTest() {
    OzoneMutableQuantiles metric = new OzoneMutableQuantiles(
            "Test_name",
            "Test_description",
            "Test_sample_name",
            "Test_value_name",
            1);

    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      int finalI = i;

      executor.execute(() -> {
        metric.add(finalI);
        if (finalI % 100 == 0) {
          metric.snapshot(getMetricsRecordBuilder());
        }
      });
    }

    Awaitility.await()
            .pollDelay(300, TimeUnit.MILLISECONDS)
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> {
              metric.snapshot(builder);
              return !builder.metrics().isEmpty();
            });

    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(1000, numOpsMetric.value().intValue());
  }
}
