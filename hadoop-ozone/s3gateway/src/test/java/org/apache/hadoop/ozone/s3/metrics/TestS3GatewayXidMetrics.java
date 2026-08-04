package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Constructor;
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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.atLeastOnce;
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
    resetMocks();
  }

  /** Re-creates fresh collector/rb mocks; used after clearMetrics() or between phases. */
  private void resetMocks() {
    collector = mock(MetricsCollector.class);
    rb = mock(MetricsRecordBuilder.class, RETURNS_SELF);
    when(collector.addRecord(anyString())).thenReturn(rb);
  }

  // === Request aggregation: bytes / count / latency / errors ===

  @Test
  @DisplayName("Bytes are grouped by XID and request type")
  void testBytesGroupedByXidAndRequestType() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "put", 200, 50, 20);
    metrics.recordRequest("xid-1", "get", 200, 30, 5);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(150L));
    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(30L));
  }

  @Test
  @DisplayName("Average latency is grouped by XID")
  void testAverageLatencyGroupedByXid() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "get", 200, 50, 30);

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("average_latency")), eq(20.0));
  }

  @Test
  @DisplayName("Request count is grouped by XID")
  void testRequestCountGroupedByXid() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);
    metrics.recordRequest("xid-1", "get", 404, 0, 20);
    metrics.recordRequest("xid-1", "get", 500, 0, 30);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(3L));
  }

  @Test
  @DisplayName("Errors are grouped by XID and error code")
  void testErrorsGroupedByXidAndErrorCode() {
    metrics.recordRequest("xid-1", "get", 404, 0, 10);
    metrics.recordRequest("xid-1", "get", 404, 0, 15);
    metrics.recordRequest("xid-1", "put", 500, 0, 20);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(2L));
    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(1L));
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 201, 204})
  @DisplayName("Success status codes are not counted as errors")
  void testSuccessStatusCodesAreNotErrors(int successCode) {
    // HTTP success codes must NOT be counted as errors (only >= ERROR_CODE_THRESHOLD).
    metrics.recordRequest("xid-1", "get", successCode, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, never()).addCounter(argThat(info -> info.name().equals("error_count")), anyLong());
  }

  @Test
  @DisplayName("Error code below 400 is not an error")
  void testErrorCodeBelow400() {
    // errorCode < 400 (HTTP success range and 3xx) must NOT be counted as an error.
    metrics.recordRequest("xid-1", "put", 399, 100, 10);
    metrics.recordRequest("xid-1", "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, never()).addCounter(argThat(info -> info.name().equals("error_count")), anyLong());
  }

  @Test
  @DisplayName("Error code 400 is counted as an error")
  void testErrorCodeAt400() {
    // errorCode == 400 must be counted as an error.
    metrics.recordRequest("xid-1", "put", 400, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("error_count")), eq(1L));
  }

  // === XID / requestType normalisation (boundary cases) ===

  @ParameterizedTest
  @NullAndEmptySource
  @DisplayName("Null/empty XID is normalised to the default value")
  void testXidNormalisedToDefault(String rawXid) {
    // null and empty XID values must both be normalised to "none".
    metrics.recordRequest(rawXid, "put", 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb, atLeastOnce()).tag(any(MetricsInfo.class), eq("none"));
    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(1L));
  }

  @Test
  @DisplayName("Null request type does not throw")
  void testNullRequestType() {
    // null requestType must not cause an NPE during recordRequest or
    // during getMetrics. NOTE: requestType is intentionally NOT normalised
    // (unlike XID) — it flows into the BytesMetricKey and the "method" tag
    // as-is, so this test only guards against NPE and the request count.
    metrics.recordRequest("xid", null, 200, 100, 10);

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")), eq(1L));
  }

  // === Percentile correctness ===

  @Test
  @DisplayName("Single sample equals all percentiles")
  void testPercentileSingleSample() {
    // For a single sample, p50/p95/p99 must all equal that sample.
    metrics.recordRequest("single-xid", "put", 200, 100, 5);

    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(5.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(5.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(5.0));
  }

  @Test
  @DisplayName("Empty samples emit no metrics")
  void testPercentileEmptySamples() {
    // After clearMetrics() in setUp there are no entries. Reading must
    // not throw an NPE and must not emit any record.
    metrics.getMetrics(collector, true);

    verify(collector, never()).addRecord(anyString());
  }

  @Test
  @DisplayName("Percentiles use linear interpolation")
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
  @DisplayName("Percentile boundary cases")
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
    resetMocks();
    metrics.clearMetrics();
    for (int i = 0; i < 100; i++) {
      metrics.recordRequest("same-xid", "put", 200, 100, 42);
    }
    metrics.getMetrics(collector, true);

    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), eq(42.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), eq(42.0));
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), eq(42.0));
  }

  // === Percentile cache ===

  @Test
  @DisplayName("Percentiles are cached between reads")
  void testPercentileCaching() {
    // First call computes percentiles and caches them.
    metrics.recordRequest("cached-xid", "put", 200, 100, 50);
    metrics.recordRequest("cached-xid", "put", 200, 100, 100);
    metrics.recordRequest("cached-xid", "put", 200, 100, 150);

    metrics.getMetrics(collector, true);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), anyDouble());
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p95")), anyDouble());
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p99")), anyDouble());

    // Re-reading without new samples must not throw (cached values reused).
    resetMocks();
    metrics.getMetrics(collector, true);
    verify(rb).addGauge(argThat(info -> info.name().equals("request_latency_ms_p50")), anyDouble());
  }

  @Test
  @DisplayName("Percentile cache is invalidated on sample overflow")
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

    // Phase 2: add many more samples up to the sample cap with a higher
    // latency. The cache must be invalidated because the sample size grew.
    resetMocks();
    int additional = metrics.getMaxLatencySamplesPerXid() - 10;
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
  @DisplayName("Percentile cache is invalidated on clear")
  void testPercentileCacheInvalidationOnClear() {
    // Populate the cache with a single latency=50 sample.
    metrics.recordRequest("phase-xid", "put", 200, 100, 50);
    metrics.getMetrics(collector, true);

    // clearMetrics() must also clear the percentile cache.
    metrics.clearMetrics();

    resetMocks();
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
  @DisplayName("Eviction removes entries when over the per-map limit")
  void testEvictionRemovesOldestKey() {
    // Eviction is throttled (every 128 records); enough records ensure the
    // map is trimmed back below the per-map limit. errorsTotal grows, then
    // is reduced by one entry per throttled eviction.
    int totalRecords = 65536;
    for (int i = 0; i < totalRecords; i++) {
      metrics.recordRequest("evict-xid", "put", 400 + i, 10, 5);
    }

    Map<?, ?> errorsTotalMap = metrics.getErrorsTotal();
    assertTrue(errorsTotalMap.size() < metrics.getMaxKeysPerMap(),
        "errorsTotal should have been evicted back below the per-map limit, got: "
            + errorsTotalMap.size());
    assertTrue(errorsTotalMap.size() > 0,
        "errorsTotal should still have entries after eviction, got: "
            + errorsTotalMap.size());
  }

  // === Cleanup window ===

  @Test
  @DisplayName("Metrics are cleared after the cleanup interval")
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
  @DisplayName("Metrics are kept before the cleanup interval")
  void testMetricsAreNotDeletedBeforeCleanupInterval() {
    metrics.recordRequest("xid-1", "put", 200, 100, 10);

    long recentTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
    metrics.setLastCleanupTime(recentTime);
    metrics.cleanupIfNeeded();

    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(100L));
    verify(rb).addGauge(argThat(info -> info.name().equals("average_latency")), eq(10.0));
  }

  @Test
  @DisplayName("clearMetrics also clears the percentile cache")
  void testClearMetricsCaches() {
    metrics.recordRequest("clear-xid", "put", 200, 100, 50);
    metrics.getMetrics(collector, true);

    metrics.clearMetrics();

    // After clear, metrics collector should return no records.
    resetMocks();
    metrics.getMetrics(collector, true);
    verify(collector, never()).addRecord(anyString());
  }

  // === XID monitoring ===

  @Test
  @DisplayName("Active XID count is emitted")
  void testXidMonitoringActiveCount() {
    metrics.recordRequest("monitor-xid", "put", 200, 100, 10);
    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("activeXidCount")), eq(1L));
  }

  @Test
  @DisplayName("Active XID count resets when metrics are cleared")
  void testXidMonitoringClearResetsCount() {
    metrics.recordRequest("clear-monitor-xid", "put", 200, 100, 10);
    metrics.getMetrics(collector, true);
    verify(rb).addCounter(argThat(info -> info.name().equals("activeXidCount")), eq(1L));

    metrics.clearMetrics();

    resetMocks();
    metrics.getMetrics(collector, true);
    // When activeXidCount == 0, writeXidMonitoringMetrics() returns early,
    // so no activeXidCount counter should be emitted.
    verify(rb, never()).addCounter(argThat(info -> info.name().equals("activeXidCount")), anyLong());
  }

  @Test
  @DisplayName("XID threshold triggers the memory ratio alert")
  void testXidMonitoringThresholdAlert() {
    // Add one more than the threshold to trigger the xidMemoryRatio alert.
    int threshold = metrics.getMaxXidMonitoringThreshold();
    for (int i = 0; i <= threshold; i++) {
      metrics.recordRequest("alert-xid-" + i, "put", 200, 10, 5);
    }

    resetMocks();
    metrics.getMetrics(collector, true);

    verify(rb).addCounter(argThat(info -> info.name().equals("activeXidCount")),
        eq((long) threshold + 1));
    // When activeXidCount > XID_MONITORING_THRESHOLD, xidMemoryRatio is emitted.
    verify(rb).addGauge(argThat(info -> info.name().equals("xidMemoryRatio")), anyDouble());
  }

  // === Equals/hashCode for nested metric keys ===

  private static Class<?> metricKeyClass(String name) throws Exception {
    return Class.forName("org.apache.hadoop.ozone.s3.metrics.S3GatewayXidMetrics$" + name);
  }

  @Test
  @DisplayName("BytesMetricKey honors the equals contract")
  void testBytesMetricKeyEquals() throws Exception {
    Class<?> keyClass = metricKeyClass("BytesMetricKey");
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
  @DisplayName("BytesMetricKey honors the hashCode contract")
  void testBytesMetricKeyHashCode() throws Exception {
    Class<?> keyClass = metricKeyClass("BytesMetricKey");
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
  @DisplayName("RequestMetricKey honors the equals contract")
  void testRequestMetricKeyEquals() throws Exception {
    Class<?> keyClass = metricKeyClass("RequestMetricKey");
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
  @DisplayName("RequestMetricKey honors the hashCode contract")
  void testRequestMetricKeyHashCode() throws Exception {
    Class<?> keyClass = metricKeyClass("RequestMetricKey");
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

  // === Singleton & lifecycle ===

  @Test
  @DisplayName("getInstance returns the same singleton")
  void testGetInstanceReturnsSameInstance() {
    S3GatewayXidMetrics instance1 = S3GatewayXidMetrics.getInstance();
    S3GatewayXidMetrics instance2 = S3GatewayXidMetrics.getInstance();
    assertSame(instance1, instance2, "getInstance() must always return the same instance");
  }

  @Test
  @DisplayName("DCL singleton is thread-safe under concurrent access")
  void testDCLSingletonThreadSafety() throws InterruptedException {
    // The DCL pattern ensures only one instance is created even under
    // concurrent getInstance() calls. The first instance is collected into
    // the array and revealed to the other threads only via happens-before
    // (the finally + latch), so we assert identity on the main thread.
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(10);
    AtomicInteger mismatchCount = new AtomicInteger(0);
    S3GatewayXidMetrics[] seen = new S3GatewayXidMetrics[10];

    for (int t = 0; t < 10; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          startLatch.await();
          S3GatewayXidMetrics instance = S3GatewayXidMetrics.getInstance();
          seen[threadId] = instance;
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

    S3GatewayXidMetrics first = seen[0];
    assertNotNull(first, "First instance must have been created");
    for (S3GatewayXidMetrics s : seen) {
      if (s != first) {
        mismatchCount.incrementAndGet();
      }
    }
    assertEquals(0, mismatchCount.get(),
        "All getInstance() calls returned the same instance (DCL singleton)");
  }

  @Test
  @DisplayName("unRegister is idempotent and leaves a usable singleton")
  void testUnregister() {
    // unRegister() must be idempotent and must not throw.
    S3GatewayXidMetrics.unRegister();
    S3GatewayXidMetrics.unRegister();

    // getInstance() re-creates/returns a usable instance after unregister.
    assertNotNull(S3GatewayXidMetrics.getInstance());
  }

  // === Concurrency ===

  private static void shutdown(ExecutorService executor) throws InterruptedException {
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
        "Executor tasks should finish within timeout");
  }

  @Test
  @DisplayName("Concurrent getMetrics and clearMetrics do not throw")
  void testGetMetricsAndClearMetricsConcurrentAccess() throws InterruptedException {
    metrics.recordRequest("race-xid", "put", 200, 100, 10);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicInteger exceptions = new AtomicInteger(0);

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
    shutdown(executor);

    assertEquals(0, exceptions.get(), "No exceptions during concurrent getMetrics/clearMetrics");
  }

  @Test
  @DisplayName("Concurrent recordRequest under the same XID loses no records")
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
    shutdown(executor);

    assertEquals(0, exceptions.get(),
        "No exceptions during concurrent recordRequest with same XID");

    long expectedBytes = (long) threadCount * recordsPerThread * 10L;
    metrics.getMetrics(collector, true);
    verify(rb).addCounter(argThat(info -> info.name().equals("sum_bytes")), eq(expectedBytes));
    verify(rb).addCounter(argThat(info -> info.name().equals("count_requests")),
        eq((long) threadCount * recordsPerThread));
  }

  @Test
  @DisplayName("Concurrent recordRequest under unique XIDs does not throw")
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
    shutdown(executor);

    assertEquals(0, exceptions.get(),
        "No ConcurrentModificationException during concurrent recordRequest with unique XIDs");

    // Reading metrics after the storm must not throw either.
    metrics.getMetrics(collector, true);
  }

  @Test
  @DisplayName("Concurrent getMetrics and recordRequest do not throw")
  void testConcurrentGetMetricsAndRecordRequest() throws InterruptedException {
    // One thread writes, another reads the same xid concurrently. The reader
    // may see partially-updated state but must never see NPE or other runtime
    // exceptions from the metric computation paths.
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
    shutdown(executor);

    assertEquals(0, exceptions.get(),
        "No NPE or runtime exceptions during concurrent read/write");
  }

  @Test
  @DisplayName("getMetrics while recording is safe")
  void testConcurrentGetMetricsWhileRecording() throws Exception {
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
    AtomicInteger readerExceptions = new AtomicInteger(0);
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
        readerExceptions.incrementAndGet();
      }
    });

    reader.start();
    allDone.await(15, TimeUnit.SECONDS);
    reader.join(5000);
    shutdown(exec);

    assertEquals(0, readerExceptions.get(),
        "No concurrent access exceptions during getMetrics while recording");
  }

  @Test
  @DisplayName("Concurrent recordRequest causes no data loss")
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
    shutdown(exec);

    // Verify that bytesTotal grew to the expected number of keys under
    // concurrent access — no records lost.
    Map<?, ?> actual = metrics.getBytesTotal();
    assertEquals(threadCount * recordsPerThread, actual.size(),
        "No data loss: all records should be present in bytesTotal under concurrent access");
  }

  // === GC intern pool ===

  @Test
  @DisplayName("BytesMetricKey intern pool reuses identical instances")
  void testBytesMetricKeyInternPooling() {
    // Creating the same key multiple times should return the same instance.
    Object key1 = metrics.internBytesKey("pool-xid", "put");
    Object key2 = metrics.internBytesKey("pool-xid", "put");
    Object key3 = metrics.internBytesKey("pool-xid", "get");
    Object key4 = metrics.internBytesKey("pool-xid", "put");

    // Same key must return the same instance.
    assertSame(key1, key2);
    assertSame(key1, key4);

    // Different requestType should return a different instance.
    assertNotEquals(key1, key3);

    // The pool should only contain 2 entries ("pool-xid \0 put" and "pool-xid \0 get").
    Map<?, ?> pool = metrics.getBytesMetricKeyPool();
    assertEquals(2, pool.size(),
        "Intern pool should contain exactly 2 distinct keys");
  }

  @Test
  @DisplayName("Intern pool is cleared together with metrics")
  void testBytesMetricKeyInternPoolClearOnClearMetrics() {
    metrics.internBytesKey("clear-pool-xid", "put");
    metrics.internBytesKey("clear-pool-xid", "get");

    metrics.clearMetrics();

    Map<?, ?> pool = metrics.getBytesMetricKeyPool();
    assertEquals(0, pool.size(),
        "bytesMetricKeyPool should be cleared after clearMetrics()");
  }

  @Test
  @DisplayName("internBytesKey is null-safe")
  void testBytesMetricKeyInternNoNPE() {
    // Interning with null fields must not throw any exception,
    // including a NullPointerException.
    try {
      metrics.internBytesKey(null, null);
      metrics.internBytesKey(null, "put");
      metrics.internBytesKey("xid", null);
    } catch (Throwable e) {
      fail("internBytesKey should not throw for null fields: " + e.getMessage());
    }
  }
}