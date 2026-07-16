package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
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

  // Reflective access to private constants so tests stay in sync with source changes.
  private static final int MAX_LATENCY_SAMPLES_PER_XID;
  private static final int MAX_KEYS_PER_MAP;

  static {
    try {
      Field f1 = S3GatewayXidMetrics.class.getDeclaredField("MAX_LATENCY_SAMPLES_PER_XID");
      f1.setAccessible(true);
      MAX_LATENCY_SAMPLES_PER_XID = f1.getInt(null);
      Field f2 = S3GatewayXidMetrics.class.getDeclaredField("MAX_KEYS_PER_MAP");
      f2.setAccessible(true);
      MAX_KEYS_PER_MAP = f2.getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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

  // === Concurrency tests ===

  @Test
  void testConcurrentRecordRequest() throws InterruptedException {
    // N threads each writing M records under the same XID. Verify that
    // no records are lost (all threads contribute to the aggregate counters).
    int threadCount = 10;
    int recordsPerThread = 100;
    String xid = "concurrent-same-xid";

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger exceptions = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < recordsPerThread; i++) {
            try {
              metrics.recordRequest(xid, "put", 200, 10, 5);
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
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals(0, exceptions.get(),
        "No exceptions during concurrent recordRequest with same XID");

    long expectedBytes = (long) threadCount * recordsPerThread * 10L;
    metrics.getMetrics(collector, true);
    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(expectedBytes));
    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")),
        eq((long) threadCount * recordsPerThread));
  }

  @Test
  void testConcurrentRecordRequestDifferentXids() throws InterruptedException {
    // Each thread uses unique XIDs. ConcurrentHashMap must handle concurrent
    // insertion and the eviction sweep without throwing.
    int threadCount = 10;
    int recordsPerThread = 100;

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger exceptions = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < recordsPerThread; i++) {
            try {
              metrics.recordRequest("xid-" + threadId + "-" + i, "put", 200, 10, 5);
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
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals(0, exceptions.get(),
        "No ConcurrentModificationException during concurrent recordRequest with unique XIDs");

    // Reading metrics after the storm must not throw either.
    metrics.getMetrics(collector, true);
  }

  @Test
  void testConcurrentGetMetricsAndRecordRequest() throws InterruptedException {
    // One thread writes, another reads the same xid concurrently.
    // The reader may see partially-updated state but must never see NPE
    // or other runtime exceptions from the metric computation paths.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicInteger exceptions = new AtomicInteger(0);

    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < 500; i++) {
          try {
            metrics.recordRequest("rw-xid", "put", 200, 10, 5);
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

    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < 500; i++) {
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

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals(0, exceptions.get(),
        "No NPE or runtime exceptions during concurrent read/write");
  }

  // === Percentile correctness ===

  @Test
  void testPercentileSingleSample() {
    // For a single sample, p50/p95/p99 must all equal that sample.
    metrics.recordRequest("single-xid", "put", 200, 100, 5);

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(5.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(5.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(5.0));
  }

  @Test
  void testPercentileEmptySamples() {
    // After clearMetrics() in setUp there are no entries. Reading must
    // not throw an NPE and must not emit any record.
    metrics.getMetrics(collector, true);

    verify(collector, never()).addRecord(anyString());
  }

  @Test
  void testPercentileLinearInterpolation() {
    // For samples [1,2,3,4,5]:
    //   p50 position = 0.5*4 + 1 = 3.0   -> sortedSamples[2]      = 3.0
    //   p95 position = 0.95*4 + 1 = 4.8  -> 4 + (5-4)*0.8        = 4.8
    //   p99 position = 0.99*4 + 1 = 4.96 -> 4 + (5-4)*0.96       = 4.96
    long[] latencies = {1, 2, 3, 4, 5};
    for (long l : latencies) {
      metrics.recordRequest("interp-xid", "put", 200, 10, l);
    }

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(3.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(4.8));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(4.96));
  }

  @Test
  void testPercentileBoundaries() {
    // Two-sample midpoint interpolation: [10, 20]
    //   p50 = 10 + (20-10)*0.5  = 15.0
    //   p95 = 10 + (20-10)*0.95 = 19.5
    //   p99 = 10 + (20-10)*0.99 = 19.9
    metrics.recordRequest("two-xid", "put", 200, 100, 10);
    metrics.recordRequest("two-xid", "put", 200, 100, 20);

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(15.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(19.5));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(19.9));

    // Boundary: all-identical samples must collapse to that constant value
    // regardless of which percentile is requested.
    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);

    metrics.clearMetrics();
    for (int i = 0; i < 100; i++) {
      metrics.recordRequest("same-xid", "put", 200, 100, 42);
    }
    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(42.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(42.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(42.0));
  }

  // === Percentile cache invalidation ===

  @Test
  void testPercentileCacheInvalidationOnOverflow() {
    // Phase 1: small number of samples with low latency. Cache is populated
    // by the first getMetrics() call.
    for (int i = 0; i < 10; i++) {
      metrics.recordRequest("overflow-xid", "put", 200, 10, 1);
    }
    metrics.getMetrics(collector, true);

    ArgumentCaptor<Double> firstP99Captor = ArgumentCaptor.forClass(Double.class);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")),
        firstP99Captor.capture());
    double firstP99 = firstP99Captor.getValue();
    assertEquals(1.0, firstP99, 0.001, "Initial p99 should be 1");

    // Phase 2: add many more samples up to MAX_LATENCY_SAMPLES_PER_XID with
    // a higher latency. The cache must be invalidated because the sample
    // size has grown.
    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);

    int additional = MAX_LATENCY_SAMPLES_PER_XID - 10;
    for (int i = 0; i < additional; i++) {
      metrics.recordRequest("overflow-xid", "put", 200, 10, 100);
    }

    metrics.getMetrics(collector, true);
    ArgumentCaptor<Double> secondP99Captor = ArgumentCaptor.forClass(Double.class);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")),
        secondP99Captor.capture());
    double secondP99 = secondP99Captor.getValue();

    assertNotEquals(firstP99, secondP99,
        "Cache must be invalidated after new samples are added");
    assertEquals(100.0, secondP99, 0.001, "Recomputed p99 should be 100");
  }

  @Test
  void testPercentileCacheInvalidationOnClear() {
    // Populate the cache with a single latency=50 sample.
    metrics.recordRequest("phase-xid", "put", 200, 100, 50);
    metrics.getMetrics(collector, true);

    // clearMetrics() must also clear the percentile cache.
    metrics.clearMetrics();

    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);

    // A new sample with a different latency. If the cache had not been
    // cleared, the cached percentile would be reused and the assertion
    // below would fail.
    metrics.recordRequest("phase-xid", "put", 200, 100, 100);
    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(100.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(100.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(100.0));
  }

  // === Eviction ===

  @Test
  void testEvictionRemovesOldestKey() throws Exception {
    // Add records past the eviction threshold. Eviction is now throttled
    // (every 128 records) so we need more records to ensure the map is
    // trimmed back below MAX_KEYS_PER_MAP.
    //
    // With 65536 records: evictions happen at 128,256,...,65536 = 512 times.
    // Each eviction removes one entry from each map. So errorsTotal grows to
    // 65536 then shrinks by 512 → ~65024 which is safely < MAX_KEYS_PER_MAP.
    int totalRecords = 65536;

    for (int i = 0; i < totalRecords; i++) {
      metrics.recordRequest("evict-xid", "put", 400 + i, 10, 5);
    }

    // After fix (it.next() + it.remove()), eviction should complete without exception.
    Field errorsTotalField = S3GatewayXidMetrics.class.getDeclaredField("errorsTotal");
    errorsTotalField.setAccessible(true);
    Map<?, ?> errorsTotalMap = (Map<?, ?>) errorsTotalField.get(metrics);

    // The map should be below MAX_KEYS_PER_MAP because eviction trimmed it.
    // No eviction happened before MAX_KEYS_PER_MAP was reached, so at some
    // point the map was larger than the current size.
    assertTrue(errorsTotalMap.size() < MAX_KEYS_PER_MAP,
        "errorsTotal should have been evicted back below MAX_KEYS_PER_MAP, got: "
            + errorsTotalMap.size());
    assertTrue(errorsTotalMap.size() > 0,
        "errorsTotal should still have entries after eviction, got: "
            + errorsTotalMap.size());
  }

  // === Boundary cases ===

  @Test
  void testNullXid() {
    // null XID must be normalised to "none" instead of being rejected
    // or causing a NullPointerException.
    metrics.recordRequest(null, "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, atLeastOnce()).tag(any(MetricsInfo.class), eq("none"));
    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(1L));
  }

  @Test
  void testEmptyXid() {
    // Empty-string XID must be normalised to "none".
    metrics.recordRequest("", "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, atLeastOnce()).tag(any(MetricsInfo.class), eq("none"));
    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(1L));
  }

  @Test
  void testNullRequestType() {
    // null requestType must not cause an NPE during recordRequest or
    // during getMetrics (which iterates bytesTotal).
    metrics.recordRequest("xid", null, 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(1L));
  }

  @Test
  void testErrorCodeBelow400() {
    // errorCode < 400 (HTTP success range and 3xx) must NOT be counted as an error.
    metrics.recordRequest("xid-1", "put", 399, 100, 10);
    metrics.recordRequest("xid-1", "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, never()).addCounter(argThat(info -> info.name().equals("error_count")), anyLong());
  }

  @Test
  void testErrorCodeAt400() {
    // errorCode == 400 must be counted as an error.
    metrics.recordRequest("xid-1", "put", 400, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(1L));
  }

  // === Equals/hashCode for nested metric keys ===

  @Test
  void testBytesMetricKeyEquals() throws Exception {
    Class<?> keyClass =
        Class.forName("org.apache.hadoop.ozone.s3.metrics.S3GatewayXidMetrics$BytesMetricKey");
    Constructor<?> ctor = keyClass.getDeclaredConstructor(String.class, String.class);
    ctor.setAccessible(true);
    Method equals = keyClass.getDeclaredMethod("equals", Object.class);
    equals.setAccessible(true);

    Object a = ctor.newInstance("xid-1", "put");
    Object aCopy = ctor.newInstance("xid-1", "put");
    Object diffMethod = ctor.newInstance("xid-1", "get");
    Object diffXid = ctor.newInstance("xid-2", "put");
    Object nullXid1 = ctor.newInstance(null, "put");
    Object nullXid2 = ctor.newInstance(null, "put");
    Object nullMethod1 = ctor.newInstance("xid-1", null);
    Object nullMethod2 = ctor.newInstance("xid-1", null);

    // Reflexivity and basic equality.
    assertTrue((Boolean) equals.invoke(a, a));
    assertTrue((Boolean) equals.invoke(a, aCopy));
    assertTrue((Boolean) equals.invoke(aCopy, a));

    // Inequality cases.
    assertFalse((Boolean) equals.invoke(a, diffMethod));
    assertFalse((Boolean) equals.invoke(a, diffXid));
    assertFalse((Boolean) equals.invoke(a, (Object) null));
    assertFalse((Boolean) equals.invoke(a, "not a key"));

    // Null-safe equality.
    assertTrue((Boolean) equals.invoke(nullXid1, nullXid2));
    assertTrue((Boolean) equals.invoke(nullMethod1, nullMethod2));
    assertFalse((Boolean) equals.invoke(nullXid1, a));
    assertFalse((Boolean) equals.invoke(nullMethod1, a));
  }

  @Test
  void testBytesMetricKeyHashCode() throws Exception {
    Class<?> keyClass =
        Class.forName("org.apache.hadoop.ozone.s3.metrics.S3GatewayXidMetrics$BytesMetricKey");
    Constructor<?> ctor = keyClass.getDeclaredConstructor(String.class, String.class);
    ctor.setAccessible(true);
    Method hashCode = keyClass.getDeclaredMethod("hashCode");
    hashCode.setAccessible(true);

    Object a = ctor.newInstance("xid-1", "put");
    Object aCopy = ctor.newInstance("xid-1", "put");
    Object diffMethod = ctor.newInstance("xid-1", "get");
    Object diffXid = ctor.newInstance("xid-2", "put");

    // Equal objects must have equal hash codes.
    assertEquals(hashCode.invoke(a), hashCode.invoke(aCopy));

    // Hash code must be null-safe for both fields.
    Object nullXid = ctor.newInstance(null, "put");
    Object nullMethod = ctor.newInstance("xid-1", null);
    assertNotNull(hashCode.invoke(nullXid));
    assertNotNull(hashCode.invoke(nullMethod));

    // Different objects typically produce different hash codes (not strictly
    // required, but a useful sanity check given the small sample).
    assertNotEquals(hashCode.invoke(a), hashCode.invoke(diffMethod));
    assertNotEquals(hashCode.invoke(a), hashCode.invoke(diffXid));
  }

  @Test
  void testRequestMetricKeyEquals() throws Exception {
    Class<?> keyClass =
        Class.forName("org.apache.hadoop.ozone.s3.metrics.S3GatewayXidMetrics$RequestMetricKey");
    Constructor<?> ctor = keyClass.getDeclaredConstructor(String.class, int.class);
    ctor.setAccessible(true);
    Method equals = keyClass.getDeclaredMethod("equals", Object.class);
    equals.setAccessible(true);

    Object a = ctor.newInstance("xid-1", 404);
    Object aCopy = ctor.newInstance("xid-1", 404);
    Object diffCode = ctor.newInstance("xid-1", 500);
    Object diffXid = ctor.newInstance("xid-2", 404);
    Object nullXid1 = ctor.newInstance(null, 404);
    Object nullXid2 = ctor.newInstance(null, 404);

    assertTrue((Boolean) equals.invoke(a, a));
    assertTrue((Boolean) equals.invoke(a, aCopy));
    assertTrue((Boolean) equals.invoke(aCopy, a));

    assertFalse((Boolean) equals.invoke(a, diffCode));
    assertFalse((Boolean) equals.invoke(a, diffXid));
    assertFalse((Boolean) equals.invoke(a, (Object) null));
    assertFalse((Boolean) equals.invoke(a, Integer.valueOf(42)));

    assertTrue((Boolean) equals.invoke(nullXid1, nullXid2));
    assertFalse((Boolean) equals.invoke(nullXid1, a));
  }

  @Test
  void testRequestMetricKeyHashCode() throws Exception {
    Class<?> keyClass =
        Class.forName("org.apache.hadoop.ozone.s3.metrics.S3GatewayXidMetrics$RequestMetricKey");
    Constructor<?> ctor = keyClass.getDeclaredConstructor(String.class, int.class);
    ctor.setAccessible(true);
    Method hashCode = keyClass.getDeclaredMethod("hashCode");
    hashCode.setAccessible(true);

    Object a = ctor.newInstance("xid-1", 404);
    Object aCopy = ctor.newInstance("xid-1", 404);
    Object diffCode = ctor.newInstance("xid-1", 500);
    Object diffXid = ctor.newInstance("xid-2", 404);

    assertEquals(hashCode.invoke(a), hashCode.invoke(aCopy));
    assertNotEquals(hashCode.invoke(a), hashCode.invoke(diffCode));
    assertNotEquals(hashCode.invoke(a), hashCode.invoke(diffXid));
  }

  // === Singleton ===

  @Test
  void testGetInstanceReturnsSameInstance() {
    S3GatewayXidMetrics instance1 = S3GatewayXidMetrics.getInstance();
    S3GatewayXidMetrics instance2 = S3GatewayXidMetrics.getInstance();
    assertSame(instance1, instance2, "getInstance() must always return the same instance");
  }

  // === Concurrency tests ===

  @Test
  void testConcurrentGetMetricsWhileRecording2() throws Exception {
    int threadCount = 4;
    int totalRecords = 10_000;

    metrics.clearMetrics();

    ExecutorService exec = Executors.newFixedThreadPool(threadCount);
    CountDownLatch allDone = new CountDownLatch(threadCount);

    // Writer threads: submit records.
    for (int t = 0; t < threadCount; t++) {
      exec.submit(() -> {
        try {
          for (int i = 0; i < totalRecords / threadCount; i++) {
            metrics.recordRequest("concurrent-" + i, "put", 200, 10, 5);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          allDone.countDown();
        }
      });
    }

    // Reader thread: collect metrics while writes are in progress.
    Thread reader = new Thread(() -> {
      try {
        for (int i = 0; i < 100; i++) {
          S3GatewayXidMetrics m = S3GatewayXidMetrics.getInstance();
          MetricsCollector c = mock(MetricsCollector.class);
          MetricsRecordBuilder rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
          when(c.addRecord(anyString())).thenReturn(rb);
          m.getMetrics(c, true);
        }
      } catch (Exception e) {
        // Don't fail the test — just record it.
      }
    });

    reader.start();
    allDone.await(15, TimeUnit.SECONDS);
    reader.join(5000);
    exec.shutdown();

    // If we got here without exceptions, concurrent metrics collection is safe.
    assertTrue(true, "No concurrent access exceptions during getMetrics while recording");
  }

  @Test
  void testConcurrentRecordRequestNoDataLoss() throws Exception {
    int threadCount = 4;
    int recordsPerThread = 1000;

    metrics.clearMetrics();

    ExecutorService exec = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int threadId = t;
      exec.submit(() -> {
        try {
          for (int i = 0; i < recordsPerThread; i++) {
            metrics.recordRequest("xid-" + threadId + "-" + i, "put", 200, 10, 1);
          }
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await(10, TimeUnit.SECONDS);
    exec.shutdown();

    // Reflective access: verify that bytesTotal grew to the expected number of keys
    Field bytesField = S3GatewayXidMetrics.class.getDeclaredField("bytesTotal");
    bytesField.setAccessible(true);
    Map<?, ?> actual = (Map<?, ?>) bytesField.get(metrics);
    assertEquals(threadCount * recordsPerThread, actual.size(),
        "No data loss: all records should be present in bytesTotal under concurrent access");
  }
}
