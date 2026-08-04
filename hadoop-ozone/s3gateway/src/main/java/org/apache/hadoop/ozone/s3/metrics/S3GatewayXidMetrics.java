package org.apache.hadoop.ozone.s3.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
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
  /** Reduced from original 10000 — 2000 samples are statistically sufficient for accurate
   *  p50/p95/p99 calculation and reduces memory from ~800MB to ~160MB at 100K unique XIDs. */
  private static final int MAX_LATENCY_SAMPLES_PER_XID = 2000;
  private static final int MAX_KEYS_PER_MAP = 100000;
  private static final long CLEANUP_INTERVAL_MS = TimeUnit.DAYS.toMillis(1);

  /** HTTP error threshold — codes >= this are counted as errors. */
  static final int ERROR_CODE_THRESHOLD = 400;

  /** XID monitoring threshold: when unique XID count exceeds this,
   *  metrics include an alert to trigger migration to approximate sketches (e.g. HdrHistogram). */
  private static final int XID_MONITORING_THRESHOLD = 5000;

  // Static metrics info constants (reuse: avoid per-call Interns.info() allocation)
  private static final MetricsInfo METRICS_INFO_XID =
      Interns.info("XID", "Request XID");
  private static final MetricsInfo METRICS_INFO_METHOD =
      Interns.info("method", "S3 request type: GET or PUT");
  private static final MetricsInfo METRICS_INFO_SUM_BYTES =
      Interns.info("sum_bytes", "Total processed bytes");
  private static final MetricsInfo METRICS_INFO_AVG_LATENCY =
      Interns.info("average_latency", "Average request latency in milliseconds");
  private static final MetricsInfo METRICS_INFO_COUNT_REQUESTS =
      Interns.info("count_requests", "Total request count");
  private static final MetricsInfo METRICS_INFO_ERROR_CODE =
      Interns.info("error_code", "HTTP error code");
  private static final MetricsInfo METRICS_INFO_ERROR_COUNT =
      Interns.info("error_count", "Total failed request count");
  private static final MetricsInfo METRICS_INFO_REQUEST_LATENCY_P50 =
      Interns.info("request_latency_ms_p50", "50th percentile latency in milliseconds");
  private static final MetricsInfo METRICS_INFO_REQUEST_LATENCY_P95 =
      Interns.info("request_latency_ms_p95", "95th percentile latency in milliseconds");
  private static final MetricsInfo METRICS_INFO_REQUEST_LATENCY_P99 =
      Interns.info("request_latency_ms_p99", "99th percentile latency in milliseconds");
  private static final MetricsInfo METRICS_INFO_ACTIVE_XID_COUNT =
      Interns.info("activeXidCount", "Number of unique XIDs tracked in memory");
  private final AtomicLong lastCleanupTime = new AtomicLong(System.currentTimeMillis());
  private static volatile S3GatewayXidMetrics instance;

  private final ConcurrentMap<BytesMetricKey, AtomicLong> bytesTotal = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> latencyMsTotal = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> latencyCount = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Deque<Long>> latencySamplesByXid = new ConcurrentHashMap<>();
  /** Version of samples for percentile caching; incremented on each add/remove. */
  private final ConcurrentMap<String, AtomicLong> samplesVersion = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> requestCount = new ConcurrentHashMap<>();
  private final ConcurrentMap<RequestMetricKey, AtomicLong> errorsTotal = new ConcurrentHashMap<>();
  /** Version counter for samples: incremented on each add() + removeFirst().
   *  Stored in array [p50, p95, p99, version]. */
  private final ConcurrentMap<String, double[]> cachedPercentiles = new ConcurrentHashMap<>();
  /** Approximate record count; used to throttle eviction checks. */
  private final AtomicLong recordCount = new AtomicLong(0);

  /** GC-intern pool for BytesMetricKey to avoid redundant allocations. */
  private final ConcurrentMap<String, BytesMetricKey> bytesMetricKeyPool = new ConcurrentHashMap<>();

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
    // ConcurrentHashMap iteration is inherently thread-safe — no lock needed.
    writeBytesMetrics(metricsCollector);
    writeLatencyMetrics(metricsCollector);
    writeRequestCountMetrics(metricsCollector);
    writeErrorMetrics(metricsCollector);
    writeXidMonitoringMetrics(metricsCollector);
    // Percentile metrics involve sorting (O(N log N)) — do outside any lock
    writeLatencyPercentileMetrics(metricsCollector);
  }

  public void recordRequest(String xid, String requestType, int errorCode, long bytes, long latencyMs) {
    cleanupIfNeeded();

    String checkedXid = checkXid(xid, "none");

    bytesTotal.computeIfAbsent(internBytesKey(checkedXid, requestType), key -> new AtomicLong())
        .addAndGet(bytes);
    latencyMsTotal.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .addAndGet(latencyMs);
    latencyCount.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .incrementAndGet();
    requestCount.computeIfAbsent(checkedXid, s -> new AtomicLong())
        .incrementAndGet();
    AtomicLong ver = samplesVersion.computeIfAbsent(checkedXid, s -> new AtomicLong());
    latencySamplesByXid.computeIfAbsent(checkedXid, s -> new ArrayDeque<>(MAX_LATENCY_SAMPLES_PER_XID))
            .add(latencyMs);
    ver.incrementAndGet();

    Deque<Long> latencySamples = latencySamplesByXid.get(checkedXid);
    synchronized (latencySamples) {
      if (latencySamples.size() > MAX_LATENCY_SAMPLES_PER_XID) {
        latencySamples.removeFirst();
        ver.incrementAndGet();
      }
    }
    if (errorCode >= ERROR_CODE_THRESHOLD) {
      RequestMetricKey requestMetricKey = new RequestMetricKey(checkedXid, errorCode);
      errorsTotal.computeIfAbsent(requestMetricKey, key -> new AtomicLong())
          .incrementAndGet();
    }

    // Throttled eviction: only check every 128 records to avoid per-request
    // overhead. 128 is chosen as a power-of-2 bitmask for minimal CPU impact.
    if ((recordCount.getAndIncrement() & 0x7F) == 0) {
      evictIfOverLimit(bytesTotal);
      evictIfOverLimit(latencyMsTotal);
      evictIfOverLimit(latencyCount);
      evictIfOverLimit(requestCount);
      evictIfOverLimit(latencySamplesByXid);
      evictIfOverLimit(errorsTotal);
    }
  }

  private void writeBytesMetrics(MetricsCollector collector) {
    for (Map.Entry<BytesMetricKey, AtomicLong> entry : bytesTotal.entrySet()) {
      BytesMetricKey key = entry.getKey();

      MetricsRecordBuilder rb = collector.addRecord(SOURCE_NAME)
          .tag(METRICS_INFO_XID, key.xid)
          .tag(METRICS_INFO_METHOD, key.requestType);

      rb.addCounter(
          METRICS_INFO_SUM_BYTES,
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
          .tag(METRICS_INFO_XID, xid)
          .addGauge(METRICS_INFO_AVG_LATENCY, avgLatencyNs);
    }
  }

  private void writeRequestCountMetrics(MetricsCollector collector) {
    for (Map.Entry<String, AtomicLong> entry : requestCount.entrySet()) {
      collector.addRecord(SOURCE_NAME)
          .tag(METRICS_INFO_XID, entry.getKey())
          .addCounter(METRICS_INFO_COUNT_REQUESTS, entry.getValue().get());
    }
  }

  private void writeErrorMetrics(MetricsCollector collector) {
    for (Map.Entry<RequestMetricKey, AtomicLong> entry : errorsTotal.entrySet()) {
      RequestMetricKey key = entry.getKey();
      collector.addRecord(SOURCE_NAME)
          .tag(METRICS_INFO_XID, key.xid)
          .tag(METRICS_INFO_ERROR_CODE, String.valueOf(key.errorCode))
          .addCounter(METRICS_INFO_ERROR_COUNT,
              entry.getValue().get());
    }
  }

  private void writeXidMonitoringMetrics(MetricsCollector collector) {
    long activeXidCount = latencySamplesByXid.size();
    if (activeXidCount == 0) {
      // No active XIDs — skip to keep metrics empty when nothing is tracked.
      return;
    }
    collector.addRecord(SOURCE_NAME)
        .addCounter(METRICS_INFO_ACTIVE_XID_COUNT, activeXidCount);

    // When the number of unique XIDs exceeds the threshold, add an alert
    // so downstream monitoring systems can trigger migration to approximate
    // sketches (e.g. HdrHistogram) instead of keeping full sample arrays.
    if (activeXidCount > XID_MONITORING_THRESHOLD) {
      double memoryRatio = (double) activeXidCount / XID_MONITORING_THRESHOLD;
      collector.addRecord(SOURCE_NAME)
          .addGauge(Interns.info("xidMemoryRatio", "Ratio of active XIDs to monitoring threshold"),
              memoryRatio);
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
    // ConcurrentHashMap.clear() is thread-safe — no lock needed.
    bytesTotal.clear();
    latencyMsTotal.clear();
    latencyCount.clear();
    requestCount.clear();
    errorsTotal.clear();
    latencySamplesByXid.clear();
    samplesVersion.clear();
    cachedPercentiles.clear();
    bytesMetricKeyPool.clear();
  }

  @VisibleForTesting
  void setLastCleanupTime(long timeMs) {
    lastCleanupTime.set(timeMs);
  }

  @VisibleForTesting
  ConcurrentMap<BytesMetricKey, AtomicLong> getBytesTotal() {
    return bytesTotal;
  }

  @VisibleForTesting
  ConcurrentMap<RequestMetricKey, AtomicLong> getErrorsTotal() {
    return errorsTotal;
  }

  @VisibleForTesting
  ConcurrentMap<String, BytesMetricKey> getBytesMetricKeyPool() {
    return bytesMetricKeyPool;
  }

  @VisibleForTesting
  int getMaxLatencySamplesPerXid() {
    return MAX_LATENCY_SAMPLES_PER_XID;
  }

  @VisibleForTesting
  int getMaxKeysPerMap() {
    return MAX_KEYS_PER_MAP;
  }

  @VisibleForTesting
  int getMaxXidMonitoringThreshold() {
    return XID_MONITORING_THRESHOLD;
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
        it.next();
        it.remove();
      }
    }
  }

  private void writeLatencyPercentileMetrics(MetricsCollector collector) {
    for (Map.Entry<String, Deque<Long>> entry : latencySamplesByXid.entrySet()) {
      String xid = entry.getKey();
      Deque<Long> samples = entry.getValue();
      AtomicLong version = samplesVersion.get(xid);
      long currentVersion = version == null ? 0L : version.get();

      double p50, p95, p99;

      // Cache percentiles: only recompute if samples version has changed.
      double[] cached = cachedPercentiles.get(xid);
      if (cached != null && (long) cached[3] == currentVersion) {
        // No new samples — reuse cached values.
        p50 = cached[0];
        p95 = cached[1];
        p99 = cached[2];
      } else {
        synchronized (samples) {
          // Recompute percentiles.
          List<Long> sortedSamples = new ArrayList<>(samples);
          Collections.sort(sortedSamples);
          p50 = percentile(sortedSamples, 50);
          p95 = percentile(sortedSamples, 95);
          p99 = percentile(sortedSamples, 99);
        }
        // Store with version snapshot taken before synchronized.
        cachedPercentiles.put(xid, new double[]{p50, p95, p99, (double) currentVersion});
      }

      MetricsRecordBuilder rb = collector.addRecord(SOURCE_NAME)
              .tag(METRICS_INFO_XID, xid);
      rb.addGauge(METRICS_INFO_REQUEST_LATENCY_P50, p50);
      rb.addGauge(METRICS_INFO_REQUEST_LATENCY_P95, p95);
      rb.addGauge(METRICS_INFO_REQUEST_LATENCY_P99, p99);
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

  /**
   * Interns a {@link BytesMetricKey} by pooling it.
   * Returns the same instance for identical (xid, requestType) pairs to reduce GC pressure.
   * Package-private for testing (test uses Object to avoid direct reference to private class).
   */
  BytesMetricKey internBytesKey(String xid, String requestType) {
    String key = xid + "\u0000" + requestType; // null-safe separator
    return bytesMetricKeyPool.computeIfAbsent(key, k -> new BytesMetricKey(xid, requestType));
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
