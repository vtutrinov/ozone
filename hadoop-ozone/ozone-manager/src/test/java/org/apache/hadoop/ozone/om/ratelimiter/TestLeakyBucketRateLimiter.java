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
package org.apache.hadoop.ozone.om.ratelimiter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Leaky Bucket Rate Limiter.
 */
public class TestLeakyBucketRateLimiter {

  @Test
  public void testAllowsUpToCapacityThenRejects() {
    LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(10, 10);

    for (int i = 0; i < 11; i++) {
      assertTrue(limiter.tryAcquire(), "Request " + i + " must be allowed");
    }
    assertFalse(limiter.tryAcquire(),
            "Request beyond capacity must be rejected");
  }

  @Test
  public void testRespectsRpsOverTime() {
    int rps = 2;
    int capacity = 2;
    LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(rps, capacity);

    int allowed = 0;
    int rejected = 0;

    long startNanos = System.nanoTime();

    long durationNanos = 0;
    while (durationNanos < 1_000_000_000L) {
      if (limiter.tryAcquire()) {
        allowed++;
      } else {
        rejected++;
      }
      durationNanos = System.nanoTime() - startNanos;
    }

    double durationSeconds = durationNanos / 1_000_000_000.0;

    double theoreticalMaxAllowed = capacity + rps * durationSeconds;

    assertTrue(allowed > 0, "Some requests must be allowed");
    assertTrue(rejected > 0, "Some requests must eventually be rejected");

    assertTrue(allowed <= theoreticalMaxAllowed + 1,
            "Allowed=" + allowed + " exceeds theoretical upper bound=" +
                    theoreticalMaxAllowed + " with tolerance=" + 1);
  }
}
