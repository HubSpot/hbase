package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A feedback‐driven limiter: adapts via contention, backpressure,
 * and over-subscription credit to converge on full utilization.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FeedbackAdaptiveRateLimiter extends RateLimiter {

  /** Base quota per window. */
  private final long baseLimit;
  /** Window duration in milliseconds. */
  private final long windowMs;

  /** Window start timestamp. */
  private long windowStart;
  /** Remaining tokens in current window. */
  private long avail;

  /** Units consumed this window. */
  private long consumed;
  /** Throttle exhaustion events this window. */
  private long exhaustionCount;

  /** Oversubscription credit (units). */
  private double credit = 0.0;
  /** Backoff multiplier (>=1.0). */
  private double backoff = 1.0;

  // configuration parameters
  private final double contentionPenaltyMultiplier;
  private final double oversubscribeRate;
  private final double maxOversubscribeFactor;
  private final double creditDecayMultiplier;
  private final double backoffRecoveryMultiplier;

  FeedbackAdaptiveRateLimiter(
    long limit,
    TimeUnit timeUnit,
    double contentionPenaltyMultiplier,
    double oversubscribeRate,
    double maxOversubscribeFactor,
    double creditDecayMultiplier,
    double backoffRecoveryMultiplier) {
    super();
    set(limit, timeUnit);
    this.baseLimit                  = limit;
    this.windowMs                   = getTimeUnitInMillis();
    this.contentionPenaltyMultiplier = contentionPenaltyMultiplier;
    this.oversubscribeRate          = oversubscribeRate;
    this.maxOversubscribeFactor     = maxOversubscribeFactor;
    this.creditDecayMultiplier      = creditDecayMultiplier;
    this.backoffRecoveryMultiplier  = backoffRecoveryMultiplier;
    initWindow();
  }

  private void initWindow() {
    windowStart     = EnvironmentEdgeManager.currentTime();
    avail           = baseLimit;
    consumed        = 0;
    exhaustionCount = 0;
  }

  @Override
  long refill(long limit) {
    // Fixed refill logic moved to getWaitInterval
    return 0;
  }

  @Override
  long getWaitInterval(long limit, long available, long amount) {
    long now     = EnvironmentEdgeManager.currentTime();
    long elapsed = now - windowStart;
    if (elapsed >= windowMs) {
      long windowsPassed = elapsed / windowMs;
      cycleWindow(windowsPassed);
    }

    // adjust avail if oversubscribed
    long effectiveQuota = baseLimit + (long) Math.floor(credit);
    if (avail < 0) {
      avail = effectiveQuota;
    }

    // grant tokens if available
    if (amount <= avail) {
      avail -= amount;
      consumed += amount;
      return 0;
    }

    // throttle exhaustion
    exhaustionCount++;
    long remainingWindow = windowMs - (now - windowStart);
    long baseWait = remainingWindow > 0 ? remainingWindow : windowMs;
    return (long) Math.ceil(baseWait * backoff);
  }

  private void cycleWindow(long windowsPassed) {
    // 1. Update oversubscription credit based on under-utilization
    double utilization = consumed / (double) baseLimit;
    double underUtilization = 1.0 - utilization;
    credit += oversubscribeRate * underUtilization * baseLimit;
    credit = Math.min((maxOversubscribeFactor - 1.0) * baseLimit, credit);
    credit *= creditDecayMultiplier;

    // 2. Update backoff based on contention
    if (exhaustionCount > 1) {
      backoff *= 1.0 + contentionPenaltyMultiplier * (exhaustionCount - 1);
    } else {
      backoff = Math.max(1.0, backoff * backoffRecoveryMultiplier);
    }
    backoff = Math.max(1.0, backoff);
    backoff = Math.min(100.0, backoff);

    // 3. Advance window start and reset counters
    windowStart += windowsPassed * windowMs;
    avail           = baseLimit;
    consumed        = 0;
    exhaustionCount = 0;
  }

  @Override
  public synchronized void consume(long amount) {
    // no-op (handled in getWaitInterval)
    super.consume(amount);
  }

  @Override
  public void setNextRefillTime(long t) {
    // unsupported
  }

  @Override
  public long getNextRefillTime() {
    // unsupported
    return -1;
  }
}
