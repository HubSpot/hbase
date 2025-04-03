package org.apache.hadoop.hbase.master.balancer;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private public class PrefixStoreFileSkewCostFunction
  extends CostFromPrefixLoadFunction {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixStoreFileSkewCostFunction.class);

  private static final String STOREFILE_SIZE_COST_KEY =
    "hbase.master.balancer.stochastic.prefixStoreFileSkewCost";
  private static final float DEFAULT_STOREFILE_SIZE_COST = 50;

  PrefixStoreFileSkewCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(STOREFILE_SIZE_COST_KEY, DEFAULT_STOREFILE_SIZE_COST));

    // TODO: remove this so we use what's in config
    this.setMultiplier(50);

    LOG.info("Initialized PrefixStoreFileSkewCostFunction");
  }

  @Override protected double getCostFromRl(BalancerRegionLoad rl) {
    return rl.getStorefileSizeMB();
  }
}
