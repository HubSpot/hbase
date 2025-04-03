package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PrefixStoreFileSkewCostFunction extends CostFromPrefixLoadFunction {

  private static final String STOREFILE_SIZE_COST_KEY =
    "hbase.master.balancer.stochastic.prefixStoreFileSkewCost";
  private static final float DEFAULT_STOREFILE_SIZE_COST = 50;

  PrefixStoreFileSkewCostFunction(Configuration conf) {
    this.setMultiplier(
        conf.getFloat(STOREFILE_SIZE_COST_KEY, DEFAULT_STOREFILE_SIZE_COST)
      );
  }

  @Override
  protected double getCostFromRl(BalancerRegionLoad rl) {
    return rl.getStorefileSizeMB();
  }
}
