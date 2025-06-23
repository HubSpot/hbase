package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Factory for FeedbackAdaptiveRateLimiter using Hadoop Configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class FeedbackAdaptiveRateLimiterFactory {

  // Keys and defaults
  public static final String QUOTA_LIMIT_KEY = RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY;
  public static final long   DEFAULT_QUOTA_LIMIT = Long.MAX_VALUE;

  public static final String REFILL_INTERVAL_MS_KEY = FixedIntervalRateLimiter.RATE_LIMITER_REFILL_INTERVAL_MS;
  public static final long   DEFAULT_REFILL_INTERVAL_MS = RateLimiter.DEFAULT_TIME_UNIT;

  public static final String CONTENTION_PENALTY_KEY =
    "hbase.quota.rate.limiter.adaptive.contention.penalty.multiplier";
  public static final double DEFAULT_CONTENTION_PENALTY = 0.5;

  public static final String OVERSUBSCRIBE_RATE_KEY =
    "hbase.quota.rate.limiter.adaptive.oversubscribe.rate";
  public static final double DEFAULT_OVERSUBSCRIBE_RATE = 0.1;

  public static final String MAX_OVERSUB_FACTOR_KEY =
    "hbase.quota.rate.limiter.adaptive.max.oversubscribe.factor";
  public static final double DEFAULT_MAX_OVERSUB_FACTOR = 1.2;

  public static final String CREDIT_DECAY_KEY =
    "hbase.quota.rate.limiter.adaptive.credit.decay.multiplier";
  public static final double DEFAULT_CREDIT_DECAY = 0.95;

  public static final String BACKOFF_RECOVERY_KEY =
    "hbase.quota.rate.limiter.adaptive.backoff.recovery.multiplier";
  public static final double DEFAULT_BACKOFF_RECOVERY = 0.9;

  private final long limit;
  private final double contentionPenalty;
  private final double oversubRate;
  private final double maxOversubFactor;
  private final double creditDecay;
  private final double backoffRecovery;

  public FeedbackAdaptiveRateLimiterFactory(Configuration conf) {
    limit = conf.getLong(QUOTA_LIMIT_KEY, DEFAULT_QUOTA_LIMIT);
    contentionPenalty = conf.getDouble(CONTENTION_PENALTY_KEY, DEFAULT_CONTENTION_PENALTY);
    oversubRate = conf.getDouble(OVERSUBSCRIBE_RATE_KEY, DEFAULT_OVERSUBSCRIBE_RATE);
    maxOversubFactor = conf.getDouble(MAX_OVERSUB_FACTOR_KEY, DEFAULT_MAX_OVERSUB_FACTOR);
    creditDecay = conf.getDouble(CREDIT_DECAY_KEY, DEFAULT_CREDIT_DECAY);
    backoffRecovery = conf.getDouble(BACKOFF_RECOVERY_KEY, DEFAULT_BACKOFF_RECOVERY);
  }

  public FeedbackAdaptiveRateLimiter create() {
    return new FeedbackAdaptiveRateLimiter(
      limit,
      TimeUnit.MILLISECONDS,
      contentionPenalty,
      oversubRate,
      maxOversubFactor,
      creditDecay,
      backoffRecovery);
  }
}
