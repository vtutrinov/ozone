package org.apache.hadoop.ozone.s3.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class maintains S3 Gateway related metrics for xid header.
 */
@Metrics(about = "S3 Gateway Metrics for xid header", context = OzoneConsts.OZONE)
public final class S3GatewayXidMetrics implements MetricsSource {

  private static final String SOURCE_NAME = S3GatewayXidMetrics.class.getSimpleName();
  private static final int MAX_LATENCY_SAMPLES_PER_XID = 10000;
  private static final int MAX_KEYS_PER_MAP = 100000;
  private static final long CLEANUP_INTERVAL_MS = TimeUnit.DAYS.toMillis(1);
  private final AtomicLong lastCleanupTime = new AtomicLong(System.currentTimeMillis());
  private final Object metricsLock = new Object();
  private static volatile S3GatewayXidMetrics instance;

  private final ConcurrentMap<BytesMetricKey, AtomicLong> bytesTotal = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> latencyMsTotal = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> latencyCount = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Deque<Long>> latencySamplesByXid = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> requestCount = new ConcurrentHashMap<>();
  private final ConcurrentMap<RequestMetricKey, AtomicLong> errorsTotal = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, double[]> cachedPercentiles = new ConcurrentHashMap<>();

  private S3GatewayXidMetrics() {
  }

  public static S3GatewayXidMetrics getInstance() {
    S3GatewayXidMetrics local = instance;
    if (local == null) {
      synchronized (S3GatewayXidMetrics.class) {
        local = instance;
        if (local == null) {
          local = new S3GatewayXidMetrics();
          instance = OzoneMetricsSystem.instance().register(SOURCE_NAME, "Metrics for xid header", local);
        }
      }
    }
    return local;
  }

  public static void unRegister() {
    OzoneMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    synchronized (metricsLock) {
      writeBytesMetrics(metricsCollector);
      writeLatencyMetrics(metricsCollector);
      writeRequestCountMetrics(metricsCollector);
      writeErrorMetrics(metricsCollector);
      writeLatencyPercentileMetrics(metricsCollector);
    }
  }

  public void recordRequest(String xid, String requestType, int errorCode, long bytes, long latencyMs) {
    cleanupIfNeeded();

    String checkedXid = checkXid(xid, "none");

    bytesTotal.computeIfAbsent(new BytesMetricKey(checkedXid, requestType), key -> new AtomicLong())
        .addAndGet(bytes);
    latencyMsTotal.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .addAndGet(latencyMs);
    latencyCount.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .incrementAndGet();
    requestCount.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .incrementAndGet();
    latencySamplesByXid.computeIfAbsent(checkedXid, s -> new ArrayDeque<>(MAX_LATENCY_SAMPLES_PER_XID))
            .add(latencyMs);

    Deque<Long> latencySamples = latencySamplesByXid.get(checkedXid);
    synchronized (latencySamples) {
      if (latencySamples.size() > MAX_LATENCY_SAMPLES_PER_XID) {
        latencySamples.removeFirst();
      }
    }
    if (errorCode >= 400) {
      RequestMetricKey requestMetricKey = new RequestMetricKey(checkedXid, errorCode);
      errorsTotal.computeIfAbsent(requestMetricKey, key -> new AtomicLong())
          .incrementAndGet();
    }

    // Evict oldest key from string maps if they exceed the max key limit.
    evictIfOverLimit(bytesTotal);
    evictIfOverLimit(latencyMsTotal);
    evictIfOverLimit(latencyCount);
    evictIfOverLimit(requestCount);
    evictIfOverLimit(latencySamplesByXid);
    evictIfOverLimit(errorsTotal);
  }

  private void writeBytesMetrics(MetricsCollector collector) {
    for (Map.Entry<BytesMetricKey, AtomicLong> entry : bytesTotal.entrySet()) {
      BytesMetricKey key = entry.getKey();

      MetricsRecordBuilder rb = collector.addRecord(SOURCE_NAME)
          .tag(Interns.info("XID", "Request XID"), key.xid)
          .tag(Interns.info("method", "S3 request type: GET or PUT"), key.requestType);

      rb.addCounter(
          Interns.info("sum_bytes", "Total processed bytes"),
          entry.getValue().get());
    }
  }

  private void writeLatencyMetrics(MetricsCollector collector) {
    for (Map.Entry<String, AtomicLong> entry : latencyCount.entrySet()) {
      String xid = entry.getKey();
      long count = entry.getValue().get();
      long total = getValue(latencyMsTotal, xid);
      double avgLatencyNs = count == 0 ? 0.0 : (double) total / count;

      collector.addRecord(SOURCE_NAME)
          .tag(Interns.info("XID", "Request XID"), xid)
          .addGauge(Interns.info("average_latency", "Average request latency in milliseconds"),
              avgLatencyNs);
    }
  }

  private void writeRequestCountMetrics(MetricsCollector collector) {
    for (Map.Entry<String, AtomicLong> entry : requestCount.entrySet()) {
      collector.addRecord(SOURCE_NAME)
          .tag(Interns.info("XID", "Request XID"), entry.getKey())
          .addCounter(Interns.info("count_requests", "Total request count"), entry.getValue().get());
    }
  }

  private void writeErrorMetrics(MetricsCollector collector) {
    for (Map.Entry<RequestMetricKey, AtomicLong> entry : errorsTotal.entrySet()) {
      RequestMetricKey key = entry.getKey();
      collector.addRecord(SOURCE_NAME)
          .tag(Interns.info("XID", "Request XID"), key.xid)
          .tag(Interns.info("error_code", "HTTP error code"), String.valueOf(key.errorCode))
          .addCounter(Interns.info("error_count", "Total failed request count"),
              entry.getValue().get());
    }
  }

  @VisibleForTesting
  void cleanupIfNeeded() {
    long now = System.currentTimeMillis();
    long lastCleanup = lastCleanupTime.get();

    if (now - lastCleanup < CLEANUP_INTERVAL_MS) {
      return;
    }

    if (lastCleanupTime.compareAndSet(lastCleanup, now)) {
      clearMetrics();
    }
  }

  @VisibleForTesting
  void clearMetrics() {
    synchronized (metricsLock) {
      bytesTotal.clear();
      latencyMsTotal.clear();
      latencyCount.clear();
      requestCount.clear();
      errorsTotal.clear();
      latencySamplesByXid.clear();
      cachedPercentiles.clear();
    }
  }

  @VisibleForTesting
  void setLastCleanupTime(long timeMs) {
    lastCleanupTime.set(timeMs);
  }

  /**
   * Evicts the first entry from the given map if its size exceeds {@link #MAX_KEYS_PER_MAP}.
   * ConcurrentHashMap does not maintain insertion order, so eviction is FIFO-ish
   * (removes whichever entry the iterator returns first).
   */
  private <K, V> void evictIfOverLimit(ConcurrentMap<K, V> map) {
    if (map.size() > MAX_KEYS_PER_MAP) {
      // ConcurrentHashMap entrySet iterator returns entries in arbitrary order,
      // but consistent enough for eviction purposes.
      Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
      if (it.hasNext()) {
        it.remove();
      }
    }
  }

  private void writeLatencyPercentileMetrics(MetricsCollector collector) {
    for (Map.Entry<String, Deque<Long>> entry : latencySamplesByXid.entrySet()) {
      String xid = entry.getKey();
      Deque<Long> samples = entry.getValue();

      double p50, p95, p99;

      // Cache percentiles: only recompute if samples have changed since last computation.
      double[] cached = cachedPercentiles.computeIfAbsent(xid, k -> new double[4]);
      long expectedSize = samples.size();
      synchronized (samples) {
        if (cached[0] == 0 && cached[1] == 0 && cached[2] == 0) {
          // First computation — calculate percentiles.
          List<Long> sortedSamples = new ArrayList<>(samples);
          Collections.sort(sortedSamples);
          p50 = percentile(sortedSamples, 50);
          p95 = percentile(sortedSamples, 95);
          p99 = percentile(sortedSamples, 99);
          cached[0] = p50;
          cached[1] = p95;
          cached[2] = p99;
          cached[3] = (double) expectedSize;
        } else if (Math.abs((double) samples.size() - cached[3]) < 0.001) {
          // No new samples — reuse cached values.
          p50 = cached[0];
          p95 = cached[1];
          p99 = cached[2];
        } else {
          // Samples added — recompute.
          List<Long> sortedSamples = new ArrayList<>(samples);
          Collections.sort(sortedSamples);
          p50 = percentile(sortedSamples, 50);
          p95 = percentile(sortedSamples, 95);
          p99 = percentile(sortedSamples, 99);
          cached[0] = p50;
          cached[1] = p95;
          cached[2] = p99;
          cached[3] = (double) expectedSize;
        }
      }

      MetricsRecordBuilder rb = collector.addRecord(SOURCE_NAME)
              .tag(Interns.info("XID", "Request XID"), xid);
      rb.addGauge(Interns.info("request_latency_ms_p50", "50th percentile latency in milliseconds"), p50);
      rb.addGauge(Interns.info("request_latency_ms_p95", "95th percentile latency in milliseconds"), p95);
      rb.addGauge(Interns.info("request_latency_ms_p99", "99th percentile latency in milliseconds"), p99);
    }
  }

  private static double percentile(List<Long> sortedSamples, double percentile) {
    if (sortedSamples == null || sortedSamples.isEmpty()) {
      return 0.0;
    }

    int n = sortedSamples.size();
    if (n == 1) {
      return sortedSamples.get(0);
    }

    double position = (percentile / 100) * (n - 1) + 1;
    int lowerIndex = (int) Math.floor(position) - 1;
    int upperIndex = (int) Math.ceil(position) - 1;

    if (lowerIndex == upperIndex) {
      return sortedSamples.get(lowerIndex);
    }

    double fraction = position - Math.floor(position);
    long lowerValue = sortedSamples.get(lowerIndex);
    long upperValue = sortedSamples.get(upperIndex);
    return lowerValue + (upperValue - lowerValue) * fraction;
  }

  private static long getValue(ConcurrentMap<String, AtomicLong> map, String key) {
    AtomicLong value = map.get(key);
    return value == null ? 0L : value.get();
  }

  private static String checkXid(String value, String defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return value;
  }

  private static final class RequestMetricKey {
    private final String xid;
    private final int errorCode;

    private RequestMetricKey(String xid, int errorCode) {
      this.xid = xid;
      this.errorCode = errorCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof RequestMetricKey)) {
        return false;
      }

      RequestMetricKey other = (RequestMetricKey) obj;
      return errorCode == other.errorCode
          && Objects.equals(xid, other.xid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(xid, errorCode);
    }
  }

  private static final class BytesMetricKey {
    private final String xid;
    private final String requestType;

    private BytesMetricKey(String xid, String requestType) {
      this.xid = xid;
      this.requestType = requestType;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof BytesMetricKey)) {
        return false;
      }

      BytesMetricKey other = (BytesMetricKey) obj;
      return Objects.equals(xid, other.xid)
          && Objects.equals(requestType, other.requestType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(xid, requestType);
    }
  }
}
