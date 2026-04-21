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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class to maintain metrics related to Rate Limiter.
 */
@Metrics(about = "Rate Limiter Metrics", context = OzoneConsts.OZONE)
public class OmRateLimiterMetrics implements MetricsSource {

  private static final String SOURCE_NAME = OmRateLimiterMetrics.class.getSimpleName();

  private static final MetricsInfo RATE_LIMITER_REQUESTS_ALLOWED = Interns.info(
        "requests_allowed",
        "Total number of allowed requests");
  private static final MetricsInfo RATE_LIMITER_REQUESTS_REJECTED = Interns.info(
        "requests_rejected",
        "Total number of rejected requests");
  private static final MetricsInfo RATE_LIMITER_CURRENT_QUOTA = Interns.info(
        "current_quota",
        "Current configured quota for rate limiter");
  private static final MetricsInfo RATE_LIMITER_REQUESTS_LAST_PERIOD = Interns.info(
          "requests_last_period",
          "Number of requests in the last period");
  private static final MetricsInfo RATE_LIMITER_OVER_LIMIT_LAST_PERIOD = Interns.info(
          "requests_over_limit_last_period",
          "Number of requests exceeding quota in the last period");
  private static final MetricsInfo RATE_LIMITER_REMAINING_QUOTA_RATIO = Interns.info(
          "remaining_quota_ratio",
          "Remaining quota ratio");

  private final ConcurrentMap<LimiterKey, Integer> allowedRequests = new ConcurrentHashMap<>();
  private final ConcurrentMap<LimiterKey, Integer> rejectedRequests = new ConcurrentHashMap<>();
  private final ConcurrentMap<LimiterKey, Integer> currentQuota = new ConcurrentHashMap<>();
  private final ConcurrentMap<LimiterKey, Integer> lastPeriodTotalRequests = new ConcurrentHashMap<>();
  private final ConcurrentMap<LimiterKey, Integer> lastPeriodOverLimitRequests = new ConcurrentHashMap<>();
  private final ConcurrentMap<LimiterKey, Double> remainingQuotaRatio = new ConcurrentHashMap<>();

  public static OmRateLimiterMetrics create() {
    OmRateLimiterMetrics omRateLimiterMetrics = new OmRateLimiterMetrics();
    return OzoneMetricsSystem.instance()
            .register(SOURCE_NAME,
                    "Metrics for Rate Limiters which show number of allowed and rejected requests, " +
                            "their amount in the last period, and ratio of remaining quota",
                    omRateLimiterMetrics);
  }

  public static void unRegister() {
    OzoneMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    buildRecords(metricsCollector, allowedRequests, RATE_LIMITER_REQUESTS_ALLOWED);
    buildRecords(metricsCollector, rejectedRequests, RATE_LIMITER_REQUESTS_REJECTED);
    buildRecords(metricsCollector, currentQuota, RATE_LIMITER_CURRENT_QUOTA);
    buildRecords(metricsCollector, lastPeriodTotalRequests, RATE_LIMITER_REQUESTS_LAST_PERIOD);
    buildRecords(metricsCollector, lastPeriodOverLimitRequests, RATE_LIMITER_OVER_LIMIT_LAST_PERIOD);
    buildRecords(metricsCollector, remainingQuotaRatio, RATE_LIMITER_REMAINING_QUOTA_RATIO);
  }

  private <T> void buildRecords(MetricsCollector metricsCollector,
                            ConcurrentMap<LimiterKey, T> limiterKeyMap,
                            MetricsInfo info) {
    for (ConcurrentMap.Entry<LimiterKey, T> entry : limiterKeyMap.entrySet()) {
      MetricsRecordBuilder recordBuilder = metricsCollector.addRecord(OmRateLimiterMetrics.class.getSimpleName())
              .setContext("ozone");
      LimiterKey key = entry.getKey();
      recordBuilder.tag(RateLimiterMetricsInfo.VolumeName, key.volume);
      recordBuilder.tag(RateLimiterMetricsInfo.BucketName, key.bucket);
      recordBuilder.tag(RateLimiterMetricsInfo.Type, key.type);

      T value = entry.getValue();
      if (value instanceof Integer) {
        recordBuilder.addGauge(info, (Integer)value);
      } else {
        recordBuilder.addGauge(info, (Double)value);
      }
      recordBuilder.endRecord();
    }
  }

  public void incAllowedRequests(String volume, String bucket, String type) {
    allowedRequests.merge(getRateLimiter(volume, bucket, type), 1, Integer::sum);
  }

  public void incRejectedRequests(String volume, String bucket, String type) {
    rejectedRequests.merge(getRateLimiter(volume, bucket, type), 1, Integer::sum);
  }

  public void updateCurrentQuota(String volume, String bucket, String type, int rps) {
    currentQuota.put(getRateLimiter(volume, bucket, type), rps);
  }

  public void updatePeriodMetrics(
          String volume, String bucket, String type, int totalRequests, int overLimitRequests, double remainingRatio) {
    LimiterKey limiterKey = getRateLimiter(volume, bucket, type);
    lastPeriodTotalRequests.put(limiterKey, totalRequests);
    lastPeriodOverLimitRequests.put(limiterKey, overLimitRequests);
    remainingQuotaRatio.put(limiterKey, remainingRatio);
  }

  public int getCurrentQuota(String volume, String bucket, String type) {
    LimiterKey limiterKey = getRateLimiter(volume, bucket, type);
    return currentQuota.getOrDefault(limiterKey, 0);
  }

  public void removeRateLimiter(String volume, String bucket, String type) {
    LimiterKey limiterKey = getRateLimiter(volume, bucket, type);
    allowedRequests.remove(limiterKey);
    rejectedRequests.remove(limiterKey);
    currentQuota.remove(limiterKey);
    lastPeriodTotalRequests.remove(limiterKey);
    lastPeriodOverLimitRequests.remove(limiterKey);
    remainingQuotaRatio.remove(limiterKey);
  }

  private LimiterKey getRateLimiter(String volume, String bucket, String type) {
    return new LimiterKey(volume, bucket, type);
  }

  private enum RateLimiterMetricsInfo implements MetricsInfo {
    VolumeName("volume_name"),
    BucketName("bucket_name"),
    Type("type");

    private final String desc;

    RateLimiterMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }

  static class LimiterKey {
    private final String volume;
    private final String bucket;
    private final String type;

    LimiterKey(String v, String b, String t) {
      volume = v;
      bucket = b;
      type = t;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LimiterKey that = (LimiterKey) o;
      return Objects.equals(volume, that.volume)
              && Objects.equals(bucket, that.bucket)
              && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(volume, bucket, type);
    }

    public String getVolume() {
      return volume;
    }

    public String getBucket() {
      return bucket;
    }

    public String getType() {
      return type;
    }
  }
}
