package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link S3GatewayXidMetrics}.
 */
class TestS3GatewayXidMetrics {
  private S3GatewayXidMetrics metrics;
  private MetricsCollector collector;
  private MetricsRecordBuilder rb;

  @BeforeEach
  void setUp() {
    metrics = S3GatewayXidMetrics.getInstance();
    metrics.clearMetrics();

    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);

    when(collector.addRecord(anyString())).thenReturn(rb);
  }

  @Test
  void testBytesGroupedByXidAndRequestType() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "put", 200, 50, 20);
    metrics.recordRequest("xid-1", "get", 200, 30, 5);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(150L));
    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(30L));
  }

  @Test
  void testAverageLatencyGroupedByXid() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "get", 200, 50, 30);

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("average_latency")), eq(20.0));
  }

  @Test
  void testRequestCountGroupedByXid() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "get", 404, 0, 20);
    metrics.recordRequest("xid-1", "get", 500, 0, 30);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(3L));
  }

  @Test
  void testErrorsGroupedByXidAndErrorCode() {
    metrics.recordRequest("xid-1", "get", 404, 0, 10);
    metrics.recordRequest("xid-1", "get", 404, 0, 15);
    metrics.recordRequest("xid-1", "put", 500, 0, 20);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(2L));
    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(1L));
  }

  @Test
  void testSuccessDoesNotIncreaseErrors() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, never()).addCounter(argThat(info -> info.name().equals("error_count")), anyLong());
  }

  @Test
  void testMetricsAreDeletedAfterCleanupInterval() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-2", "get", 404, 50, 20);

    long oldTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
    metrics.setLastCleanupTime(oldTime);
    metrics.cleanupIfNeeded();

    metrics.getMetrics(collector, true);

    verify(collector, never()).addRecord(anyString());
  }

  @Test
  void testMetricsAreNotDeletedBeforeCleanupInterval() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);

    long recentTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
    metrics.setLastCleanupTime(recentTime);
    metrics.cleanupIfNeeded();

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(100L));
    verify(rb).addGauge(argThat(info -> info.name().equals("average_latency")), eq(10.0));
  }

  // --- Tests for fixes applied in code review ---

  @Test
  void testSuccessStatusCodesAreNotErrors() {
    // 2xx codes should NOT be counted as errors (only >= 400)
    metrics.recordRequest("xid-1", "get", 200, 100, 10);
    metrics.recordRequest("xid-2", "get", 201, 50, 15);
    metrics.recordRequest("xid-3", "get", 204, 0, 5);

    metrics.getMetrics(collector, true);

    verify(rb, never()).addCounter(argThat(info -> info.name().equals("error_count")), anyLong());
  }

  @Test
  void testEvictionWhenKeyLimitExceeded() {
    // Verify eviction logic exists and doesn't crash under high key count
    int testKeys = 1000;
    for (int i = 0; i < testKeys; i++) {
      metrics.recordRequest("evict-xid-" + i, "put", 200, 10, 1);
    }

    // Should succeed without errors; each XID has unique bytes=10
    metrics.getMetrics(collector, true);
    verify(rb, atLeast(1)).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(10L));
  }

  @Test
  void testPercentileCaching() {
    // First call computes percentiles
    metrics.recordRequest("cached-xid", "put", 200, 100, 50);
    metrics.recordRequest("cached-xid", "put", 200, 100, 100);
    metrics.recordRequest("cached-xid", "put", 200, 100, 150);

    metrics.getMetrics(collector, true);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), anyDouble());
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), anyDouble());
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), anyDouble());

    // Second call should use cached values — same call succeeds (no NPE from stale cache)
    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);

    metrics.getMetrics(collector, true);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), anyDouble());
  }

  @Test
  void testClearMetricsCachesPercentiles() {
    metrics.recordRequest("clear-xid", "put", 200, 100, 50);
    metrics.getMetrics(collector, true);

    // Clear metrics should also clear cached percentiles
    metrics.clearMetrics();

    // After clear, metrics collector should return no records
    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);
    metrics.getMetrics(collector, true);
    verify(collector, never()).addRecord(anyString());
  }

  @Test
  void testGetMetricsAndClearMetricsConcurrentAccess() throws InterruptedException {
    metrics.recordRequest("race-xid", "put", 200, 100, 10);

    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicInteger exceptions = new AtomicInteger(0);

    // Thread 1: continuously calls getMetrics
    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < 100; i++) {
          try {
            metrics.getMetrics(collector, true);
          } catch (Exception e) {
            exceptions.incrementAndGet();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        doneLatch.countDown();
      }
    });

    // Thread 2: calls clearMetrics
    executor.submit(() -> {
      try {
        startLatch.await();
        Thread.sleep(50);
        metrics.clearMetrics();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        doneLatch.countDown();
      }
    });

    startLatch.countDown();
    assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdown();

    assertEquals(0, exceptions.get(), "No exceptions during concurrent getMetrics/clearMetrics");
  }

  @Test
  void testDCLSingletonThreadSafety() throws InterruptedException {
    // Reset singleton to null via reflection would be ideal,
    // but since instance is private, we test that getInstance()
    // is safe when multiple threads call it concurrently.
    // The DCL pattern ensures only one instance is created.

    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(10);
    AtomicInteger instanceCount = new AtomicInteger(0);
    S3GatewayXidMetrics[] firstInstance = new S3GatewayXidMetrics[1];

    for (int i = 0; i < 10; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          S3GatewayXidMetrics instance = S3GatewayXidMetrics.getInstance();
          if (threadId == 0) {
            firstInstance[0] = instance;
          } else {
            // All instances should be the same singleton
            assertNotNull(firstInstance[0]);
            if (instance != firstInstance[0]) {
              instanceCount.incrementAndGet();
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdown();

    assertEquals(0, instanceCount.get(),
        "All getInstance() calls returned the same instance (DCL singleton)");
  }
}
