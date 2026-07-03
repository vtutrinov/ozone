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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Rate limiter request counters stored in a dedicated file (separate from
 * the OmMetricsInfo file, so older OM versions can still parse that one),
 * which will be used during OM restart to initialize the rate limiter
 * metrics.
 */
public class OmRateLimiterMetricsInfo {

  @JsonProperty
  private List<RateLimiterMetric> rateLimiterMetrics;

  OmRateLimiterMetricsInfo() {
    this.rateLimiterMetrics = new ArrayList<>();
  }

  public List<RateLimiterMetric> getRateLimiterMetrics() {
    return rateLimiterMetrics;
  }

  public void setRateLimiterMetrics(List<RateLimiterMetric> rateLimiterMetrics) {
    this.rateLimiterMetrics = rateLimiterMetrics;
  }

  /**
   * Persisted request counters of a single rate limiter.
   */
  public static class RateLimiterMetric {

    @JsonProperty
    private String volume;

    @JsonProperty
    private String bucket;

    @JsonProperty
    private String type;

    @JsonProperty
    private long allowedRequests;

    @JsonProperty
    private long rejectedRequests;

    public String getVolume() {
      return volume;
    }

    public void setVolume(String volume) {
      this.volume = volume;
    }

    public String getBucket() {
      return bucket;
    }

    public void setBucket(String bucket) {
      this.bucket = bucket;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public long getAllowedRequests() {
      return allowedRequests;
    }

    public void setAllowedRequests(long allowedRequests) {
      this.allowedRequests = allowedRequests;
    }

    public long getRejectedRequests() {
      return rejectedRequests;
    }

    public void setRejectedRequests(long rejectedRequests) {
      this.rejectedRequests = rejectedRequests;
    }
  }
}
