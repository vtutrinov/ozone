/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.RateLimiterInfo;
import org.apache.hadoop.ozone.om.ratelimiter.LeakyBucketRateLimiter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType.READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RateLimiterManager}, specifically for metrics cases.
 */
public class TestRateLimiterManagerMetrics {

  private OmRateLimiterMetrics metrics;
  private LeakyBucketRateLimiter limiter;
  private RateLimiterManager manager;
  private OMMetadataManager metadataManager;

  @BeforeEach
  public void setup() throws IOException {
    metrics = mock(OmRateLimiterMetrics.class);
    limiter = mock(LeakyBucketRateLimiter.class);
    metadataManager = mock(OMMetadataManager.class);

    when(metadataManager.getBucketKey(anyString(), anyString())).thenReturn("bucket-key");

    Table table = mock(Table.class);
    TableIterator iter = mock(TableIterator.class);

    Table.KeyValue kv = mock(Table.KeyValue.class);
    RateLimiterInfo rli = mock(RateLimiterInfo.class);
    when(rli.getBucketName()).thenReturn("bucket1");
    when(rli.getVolumeName()).thenReturn("vol1");
    when(rli.getType()).thenReturn(READ);
    when(rli.getRps()).thenReturn(4);
    when(kv.getValue()).thenReturn(rli);
    when(iter.hasNext()).thenReturn(true, false);
    when(table.iterator()).thenReturn(iter);
    when(metadataManager.getRateLimiterInfoTable()).thenReturn(table);
    when(iter.next()).thenReturn(kv);

    manager = spy(new RateLimiterManager(metadataManager, metrics));
    doReturn(limiter).when(manager).getLimiter(any(RateLimiterManager.BucketLimiters.class),
        any(RateLimiterType.class));
  }

  @Test
  public void testTryAcquireAllowedRequestsUpdatesMetrics() {
    String vol = "vol";
    String bucket = "bucket";
    RateLimiterType type = READ;

    when(limiter.tryAcquire()).thenReturn(true);
    when(metrics.getCurrentQuota(vol, bucket, type.name())).thenReturn(10);

    boolean result = manager.tryAcquire(vol, bucket, type);

    assertTrue(result);

    verify(limiter).tryAcquire();
    verify(metrics).incAllowedRequests(vol, bucket, type.name());
    verify(metrics, never()).incRejectedRequests(any(), any(), any());
    verify(metrics).updatePeriodMetrics(eq(vol), eq(bucket), eq(type.name()), anyInt(), anyInt(), anyDouble());
  }

  @Test
  public void testTryAcquireUsingInTheSamePeriod() {
    String vol = "vol";
    String bucket = "bucket";
    RateLimiterType type = READ;

    when(limiter.tryAcquire()).thenReturn(true, true, true, false);
    // CurrentQuota is used only to compute remainingQuotaRatio for each period and tryAcquire is not enough
    // for that calculation so we need to mock quota as well.
    when(metrics.getCurrentQuota(any(), any(), any())).thenReturn(4);

    manager.tryAcquire(vol, bucket, type);
    manager.tryAcquire(vol, bucket, type);
    manager.tryAcquire(vol, bucket, type);
    manager.tryAcquire(vol, bucket, type);

    verify(metrics, times(3)).incAllowedRequests(vol, bucket, type.name());
    verify(metrics, times(1)).incRejectedRequests(vol, bucket, type.name());

    ArgumentCaptor<Double> ratioCaptor = ArgumentCaptor.forClass(Double.class);

    verify(metrics, times(4)).updatePeriodMetrics(
            eq(vol),
            eq(bucket),
            eq(type.name()),
            anyInt(),
            anyInt(),
            ratioCaptor.capture()
    );

    assertEquals(0.75, ratioCaptor.getAllValues().get(0), 0.0001);
    assertEquals(0.5, ratioCaptor.getAllValues().get(1), 0.0001);
    assertEquals(0.25, ratioCaptor.getAllValues().get(2), 0.0001);
    assertEquals(0.0, ratioCaptor.getAllValues().get(3), 0.0001);
  }

  @Test
  public void testGetOrRollPeriodChangesCurrentToLast() {
    String vol = "vol";
    String bucket = "bucket";
    String type = "read";
    int currentTotal = 3;
    int currentRejected = 1;

    RateLimiterManager.PeriodState state1 = manager.getOrRollPeriod(vol, bucket, type, 100);
    state1.setCurrentTotal(currentTotal);
    state1.setCurrentRejected(currentRejected);

    RateLimiterManager.PeriodState state2 = manager.getOrRollPeriod(vol, bucket, type, 101);

    assertSame(state1, state2);
    assertEquals(currentTotal, state2.getLastTotal());
    assertEquals(currentRejected, state2.getLastRejected());
    assertEquals(0, state2.getCurrentTotal());
    assertEquals(0, state2.getCurrentRejected());
    assertEquals(101, state2.getPeriodSecond());
  }
}
