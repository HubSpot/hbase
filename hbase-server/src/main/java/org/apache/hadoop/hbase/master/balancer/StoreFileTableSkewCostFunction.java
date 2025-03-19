package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cost function to evaluate cluster imbalance based on store file sizes per table.
 * Assumes that the cluster state provides a store file size metric per server for every table.
 */
@InterfaceAudience.Private
class StoreFileTableSkewCostFunction extends CostFunction {

  private static final String STOREFILE_TABLE_SKEW_COST_KEY =
    "hbase.master.balancer.stochastic.storeFileTableSkewCost";
  private static final float DEFAULT_STORE_FILE_TABLE_SKEW_COST = 1000f;

  DoubleArrayCost[] costsPerTable;

  public StoreFileTableSkewCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(STOREFILE_TABLE_SKEW_COST_KEY,
      DEFAULT_STORE_FILE_TABLE_SKEW_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    costsPerTable = new DoubleArrayCost[cluster.numTables];
    for (int tableIdx = 0; tableIdx < cluster.numTables; tableIdx++) {
      costsPerTable[tableIdx] = new DoubleArrayCost();
      costsPerTable[tableIdx].prepare(cluster.numServers);
      final int tableIndex = tableIdx;
      costsPerTable[tableIdx].applyCostsChange(costs -> {
        // For each server, set the cost to be the store file size for this table.
        // Assume the cluster state provides storeFileSizePerServerPerTable[tableIndex][server] values.
        for (int i = 0; i < cluster.numServers; i++) {
          costs[i] = cluster.storeFileSizePerServerPerTable[tableIndex][i];
        }
      });
    }
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    int tableIdx = cluster.regionIndexToTableIndex[region];
    costsPerTable[tableIdx].applyCostsChange(costs -> {
      costs[oldServer] = cluster.storeFileSizePerServerPerTable[tableIdx][oldServer];
      costs[newServer] = cluster.storeFileSizePerServerPerTable[tableIdx][newServer];
    });
  }

  @Override
  protected double cost() {
    double totalCost = 0;
    for (int tableIdx = 0; tableIdx < cluster.numTables; tableIdx++) {
      totalCost += costsPerTable[tableIdx].cost();
    }
    return totalCost;
  }
}
