/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratelimiter;

import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_READ_CMDS;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_WRITE_CMDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests ensuring rate limiter configuration stays in sync
 * with OMRequest cmd types.
 */
public class TestRateLimiterCoverage {

  /**
   * Guard test to keep OMRequest {@link
   * org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type}
   * in sync with rate-limited commands.
   *
   * <p>If this test fails after adding a new Type:
   * <ul>
   *   <li>If the new Type is <b>not</b> bucket-related, increment
   *       {@code notSupportedCount}.</li>
   *   <li>If the new Type <b>is</b> bucket-related and should be
   *       rate-limited, add it to
   *       {@link org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper#RATE_LIMITED_READ_CMDS}
   *       or
   *       {@link org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper#RATE_LIMITED_WRITE_CMDS},
   *       and update
   *       {@link org.apache.hadoop.ozone.om.RateLimiterManager#extractVolumeBucket}.</li>
   * </ul>
   */
  @Test
  public void testOmRequestTypeCountGuard() {
    final int currentSupportedCount = RATE_LIMITED_READ_CMDS.size() + RATE_LIMITED_WRITE_CMDS.size();
    // Internal requests without a client bucket (e.g. the SDPOZN-1979 bucket raft group assignment requests)
    // are exempt from rate limiting.
    final int notSupportedCount = 65;
    final int expectedTypeCount = OzoneManagerProtocolProtos.Type.values().length - notSupportedCount;

    assertEquals(expectedTypeCount,
            currentSupportedCount,
            "OMRequest Type enum size changed. If a new Type was added, " +
                    "review and update RateLimiterManager.");
  }
}
