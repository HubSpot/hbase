package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * With this limiter resources will be refilled only after a fixed interval of time,
 * with adaptive-wait, EWMA smoothing, and a PID-based feedback controller with
 * derivative damping, anti-windup, step-rate limiting, and jitter.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedIntervalRateLimiter extends RateLimiter {

  public static final String RATE_LIMITER_REFILL_INTERVAL_MS =
    "hbase.quota.rate.limiter.refill.interval.ms";
  public static final String RATE_LIMITER_REFILL_INTERVAL_ADAPTIVE =
    "hbase.quota.rate.limiter.refill.interval.adaptive";

  private final AtomicLong nextRefillTime = new AtomicLong(-1L);
  private final long refillInterval;
  private final boolean adaptiveWaitEnabled;

  private final AtomicLong lastRefillTime = new AtomicLong(-1L);
  private final LongAdder violationsInCurrentInterval = new LongAdder();
  private final LongAdder consumedInCurrentInterval = new LongAdder();

  // PID controller state
  private volatile double smoothedMultiplier = 1.0;
  private volatile double errorIntegral = 0.0;
  private volatile double lastError = 0.0;

  // PID gains
  private static final double KP = 0.35;      // proportional gain
  private static final double KI = 0.05;      // integral gain
  private static final double KD = 0.02;      // derivative gain

  // step-rate limits per interval
  private static final double MAX_STEP_UP = 1.2;
  private static final double MAX_STEP_DOWN = 0.8;

  // multiplier bounds
  private static final double MIN_MULTIPLIER = 0.01;
  private static final double MAX_MULTIPLIER = 100.0;

  // smoothing factor for multiplier updates
  private static final double MULTIPLIER_EWMA_ALPHA = 0.3;

  public FixedIntervalRateLimiter() {
    this(DEFAULT_TIME_UNIT, false);
  }

  public FixedIntervalRateLimiter(long refillInterval, boolean adaptiveWaitEnabled) {
    super();
    Preconditions.checkArgument(
      getTimeUnitInMillis() >= refillInterval,
      String.format(
        "Refill interval %s must be <= TimeUnit millis %s",
        refillInterval, getTimeUnitInMillis()));
    this.refillInterval = refillInterval;
    this.adaptiveWaitEnabled = adaptiveWaitEnabled;
  }

  @Override
  public synchronized void consume(long amount) {
    super.consume(amount);
    if (adaptiveWaitEnabled) {
      consumedInCurrentInterval.add(amount);
    }
  }

  @Override
  public long refill(long limit) {
    long now = EnvironmentEdgeManager.currentTime();
    long nextAt = nextRefillTime.get();

    if (nextAt == -1L) {
      nextRefillTime.compareAndSet(-1L, now + refillInterval);
      return limit;
    }
    if (now < nextAt) {
      return 0;
    }

    // At refill boundary: closed-loop PID correction
    long observedLast = lastRefillTime.get();
    if (observedLast != nextAt && adaptiveWaitEnabled) {
      if (lastRefillTime.compareAndSet(observedLast, nextAt)) {
        long used = consumedInCurrentInterval.sumThenReset();
        violationsInCurrentInterval.reset();

        double windowFrac =
          (refillInterval + now - nextAt) / (double) getTimeUnitInMillis();
        long desired = (long) (limit * windowFrac);
        double actual = Math.max(1.0, (double) used);
        double error = (double) desired / actual - 1.0;

        // anti-windup: only integrate when not at bounds
        if (smoothedMultiplier > MIN_MULTIPLIER && smoothedMultiplier < MAX_MULTIPLIER) {
          errorIntegral += error;
        }
        double errorDelta = error - lastError;
        lastError = error;

        // raw factor from PID
        double rawFactor = 1.0 + KP * error + KI * errorIntegral + KD * errorDelta;
        // limit rate-of-change
        rawFactor = Math.max(rawFactor, MAX_STEP_DOWN);
        rawFactor = Math.min(rawFactor, MAX_STEP_UP);

        // apply to multiplier, then smooth
        double candidate = smoothedMultiplier * rawFactor;
        double updated = MULTIPLIER_EWMA_ALPHA * candidate
          + (1 - MULTIPLIER_EWMA_ALPHA) * smoothedMultiplier;
        // clamp to global bounds
        smoothedMultiplier = Math.max(MIN_MULTIPLIER,
          Math.min(MAX_MULTIPLIER, updated));
      }
    }

    long diff = refillInterval + now - nextAt;
    long refills = diff / refillInterval;
    nextRefillTime.set(now + refillInterval);

    long refillAmount = refills * getRefillIntervalAdjustedLimit(limit);
    return Math.min(limit, refillAmount);
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    limit = getRefillIntervalAdjustedLimit(limit);
    long curr = nextRefillTime.get();
    if (curr == -1L) {
      return 0;
    }

    long now = EnvironmentEdgeManager.currentTime();
    long diff = amount - available;
    long untilNext = curr - now;

    if (diff <= limit) {
      return untilNext > 0 ? applyAdaptiveWait(untilNext) : 0;
    }

    long extra = diff / limit;
    if (diff % limit == 0) {
      extra--;
    }
    long baseWait = untilNext + extra * refillInterval;
    return baseWait > 0 ? applyAdaptiveWait(baseWait) : 0;
  }

  private long applyAdaptiveWait(long baseWait) {
    if (!adaptiveWaitEnabled) {
      return baseWait;
    }

    violationsInCurrentInterval.increment();
    int v = (int) violationsInCurrentInterval.sum();
    double raw = (v <= 10)
      ? 0.1 + (v - 1) * (0.9 / 9)
      : Math.min(Math.pow(1.1, v - 10), MAX_MULTIPLIER);

    double smooth;
    synchronized (this) {
      smooth = 0.5 * raw + 0.5 * smoothedMultiplier;
      smoothedMultiplier = Math.max(MIN_MULTIPLIER,
        Math.min(MAX_MULTIPLIER, smooth));
      smooth = smoothedMultiplier;
    }

    long wait = (long) Math.ceil(baseWait * smooth);
    long jitter = ThreadLocalRandom.current()
      .nextLong(-baseWait / 20, baseWait / 20 + 1);
    return Math.max(0, wait + jitter);
  }

  private long getRefillIntervalAdjustedLimit(long limit) {
    return (long) Math.ceil(
      refillInterval / (double) getTimeUnitInMillis() * limit);
  }

  @Override
  public void setNextRefillTime(long t) {
    nextRefillTime.set(t);
  }

  @Override
  public long getNextRefillTime() {
    return nextRefillTime.get();
  }

  long getViolationsInCurrentInterval() {
    return violationsInCurrentInterval.sum();
  }
}
