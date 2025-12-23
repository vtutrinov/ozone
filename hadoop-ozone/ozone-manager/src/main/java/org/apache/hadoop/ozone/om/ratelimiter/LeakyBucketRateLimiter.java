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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RateLimiter implementation based on the leaky bucket algorithm.
 */
public class LeakyBucketRateLimiter implements RateLimiter {

  private final double capacity;
  private final double leakRatePerNano;

  private double water;
  private long lastUpdateNanos;
  private final Lock lock = new ReentrantLock();

  public LeakyBucketRateLimiter(int rps, int capacity) {
    if (rps <= 0) {
      throw new IllegalArgumentException("rps must be > 0");
    }
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0");
    }
    this.capacity = capacity;
    this.leakRatePerNano = (double) rps / 1_000_000_000L;
    this.water = 0.0;
    this.lastUpdateNanos = System.nanoTime();
  }

  public LeakyBucketRateLimiter(int rps) {
    this(rps, rps);
  }

  @Override
  public boolean tryAcquire() {
    lock.lock();
    try {
      long now = System.nanoTime();
      leakOldRequests(now);

      if (water >= capacity) {
        return false;
      }

      water += 1.0;
      return true;
    } finally {
      lock.unlock();
    }
  }

  private void leakOldRequests(long nowNanos) {
    long elapsed = nowNanos - lastUpdateNanos;
    if (elapsed <= 0) {
      return;
    }
    double leaked = elapsed * leakRatePerNano;
    water = Math.max(0.0, water - leaked);
    lastUpdateNanos = nowNanos;
  }
}
