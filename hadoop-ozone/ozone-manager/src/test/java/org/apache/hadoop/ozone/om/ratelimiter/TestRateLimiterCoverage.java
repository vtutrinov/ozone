package org.apache.hadoop.ozone.om.ratelimiter;

import org.junit.jupiter.api.Test;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_READ_CMDS;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_WRITE_CMDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    final int notSupportedCount = 57;
    final int expectedTypeCount = OzoneManagerProtocolProtos.Type.values().length - notSupportedCount;

    assertEquals(expectedTypeCount,
            currentSupportedCount,
            "OMRequest Type enum size changed. If you added a new Type, " +
                    "review and update RateLimiterManager.");
  }
}
