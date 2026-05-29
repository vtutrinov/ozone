package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
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
}
