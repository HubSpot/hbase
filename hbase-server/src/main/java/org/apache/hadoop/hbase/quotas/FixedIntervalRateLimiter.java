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
package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

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

/**
 * With this limiter resources will be refilled only after a fixed interval of time.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedIntervalRateLimiter extends RateLimiter {

  /**
   * The FixedIntervalRateLimiter can be harsh from a latency/backoff perspective, which makes it
   * difficult to fully and consistently utilize a quota allowance. By configuring the
   * refill interval to a lower value you will encourage the rate limiter
   * to throw smaller wait intervals for requests which may be fulfilled in timeframes shorter than
   * the quota's full interval. For example, if you're saturating a 100MB/sec read IO quota with a
   * ton of tiny gets, then configuring this to a value like 100ms will ensure that your retry
   * backoffs approach ~100ms, rather than 1sec. Be careful not to configure this too low, or you
   * may produce a dangerous amount of retry volume.
   */
  public static final String RATE_LIMITER_REFILL_INTERVAL_MS =
    "hbase.quota.rate.limiter.refill.interval.ms";

  /**
   * Controls whether adaptive wait intervals are enabled for the FixedIntervalRateLimiter.
   * When enabled, the rate limiter will adjust wait times based on quota violation patterns
   * to improve quota utilization and reduce unnecessary throttling.
   */
  public static final String RATE_LIMITER_REFILL_INTERVAL_ADAPTIVE =
    "hbase.quota.rate.limiter.refill.interval.adaptive";

  private final AtomicLong nextRefillTime = new AtomicLong(-1L);
  private final long refillInterval;
  private final boolean adaptiveWaitEnabled;

  private final LongAdder violationsInCurrentInterval = new LongAdder();
  private volatile long lastRefillTime = -1L;

  public FixedIntervalRateLimiter() {
    this(DEFAULT_TIME_UNIT, false);
  }

  public FixedIntervalRateLimiter(long refillInterval, boolean adaptiveWaitEnabled) {
    super();
    Preconditions.checkArgument(getTimeUnitInMillis() >= refillInterval,
      String.format("Refill interval %s must be less than or equal to TimeUnit millis %s",
        refillInterval, getTimeUnitInMillis()));
    this.refillInterval = refillInterval;
    this.adaptiveWaitEnabled = adaptiveWaitEnabled;
  }

  @Override
  public long refill(long limit) {
    final long now = EnvironmentEdgeManager.currentTime();
    long nextRefillAt = nextRefillTime.get();

    if (nextRefillAt == -1L) {
      nextRefillTime.compareAndSet(-1L, now + refillInterval);
      return limit;
    }
    if (now < nextRefillAt) {
      return 0;
    }

    // Reset violations counter on new refill boundary
    if (lastRefillTime != nextRefillAt) {
      violationsInCurrentInterval.reset();
      lastRefillTime = nextRefillAt;
    }

    long diff = refillInterval + now - nextRefillAt;
    long refills = diff / refillInterval;
    nextRefillTime.compareAndSet(nextRefillAt, now + refillInterval);
    long refillAmount = refills * getRefillIntervalAdjustedLimit(limit);
    return Math.min(limit, refillAmount);
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    // adjust the limit based on the refill interval
    limit = getRefillIntervalAdjustedLimit(limit);

    long curr = nextRefillTime.get();
    if (curr == -1L) {
      return 0;
    }
    final long now = EnvironmentEdgeManager.currentTime();
    long diff = amount - available;
    long nextRefillInterval = curr - now;

    if (diff <= limit) {
      if (nextRefillInterval > 0) {
        return applyAdaptiveWait(nextRefillInterval);
      }
      // No wait needed
      return 0;
    }

    // Otherwise, compute how many extra refill cycles are needed.
    long extra = diff / limit;
    if (diff % limit == 0) {
      extra--;
    }
    long baseWait = nextRefillInterval + (extra * refillInterval);

    if (baseWait > 0) {
      return applyAdaptiveWait(baseWait);
    }
    return 0;
  }

  /**
   * Applies the adaptive multiplier to the base wait interval.
   * Uses violations in the current refill interval to derive a multiplier:
   * - Few violations (0-1): multiplier < 1 (reduces wait times)
   * - Many violations (2+): multiplier > 1 (increases wait times)
   */
  private long applyAdaptiveWait(long baseWait) {
    violationsInCurrentInterval.increment();
    
    if (!adaptiveWaitEnabled) {
      return baseWait;
    }
    
    int violations = (int) violationsInCurrentInterval.sum();
    double multiplier;
    
    if (violations == 1) {
      // First violation in interval: reduce wait time to encourage utilization
      multiplier = 0.5;
    } else if (violations == 2) {
      // Second violation: use base wait time
      multiplier = 1.0;
    } else {
      // Multiple violations: increase wait time exponentially to prevent overload
      // Each additional violation adds 50%, capped at 100x
      multiplier = Math.min(1.0 + (violations - 2) * 0.5, 100.0);
    }
    
    return (long) Math.ceil(baseWait * multiplier);
  }

  private long getRefillIntervalAdjustedLimit(long limit) {
    return (long) Math.ceil(refillInterval / (double) getTimeUnitInMillis() * limit);
  }

  // This method is for strictly testing purpose only
  @Override
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime.set(nextRefillTime);
  }

  @Override
  public long getNextRefillTime() {
    return nextRefillTime.get();
  }

  /**
   * Get the current violations in the current interval count. Primarily for testing.
   */
  long getViolationsInCurrentInterval() {
    return violationsInCurrentInterval.sum();
  }
}
