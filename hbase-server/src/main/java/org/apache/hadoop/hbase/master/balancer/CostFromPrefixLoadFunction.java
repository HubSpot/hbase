package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AtomicDouble;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class the allows writing costs functions from rolling average of some number from
 * prefix load.
 */
abstract class CostFromPrefixLoadFunction extends CostFunction {

  /*
   * Each prefix is served by a subset of servers. The average size per server is the total size
   * of all regions for the prefix, divided by the size of that subset. The cost is a function of how
   * far the actual amount of a prefix on a server is from the average - an ideal distribution
   * will have every server hosting a prefix have exactly the average size (i.e. a perfectly
   * uniform distribution). The DoubleCostArray summarizes the storefile sizes with a variant
   * of the root mean square deviation to represent this.
   */
  private final Map<Short, DoubleArrayCost> costPerPrefix = new ConcurrentHashMap<>();
  private final Multimap<Short, Integer> regionsByPrefix = HashMultimap.create();

  @Override void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);

    regionsByPrefix.clear();
    for (int region = 0; region < cluster.numRegions; region++) {
      for (short prefix : HubSpotCellUtilities.toCells(cluster.regions[region].getStartKey(),
        cluster.regions[region].getEndKey(), HubSpotCellUtilities.MAX_CELL_COUNT)) {
        regionsByPrefix.put(prefix, region);
      }
    }

    for (short prefix = 0; prefix < HubSpotCellUtilities.MAX_CELL_COUNT; prefix++) {
      updateStoreFilePerPrefixCosts(prefix);
    }
  }

  @Override protected void regionMoved(int regionIndex, int oldServer, int newServer) {
    RegionInfo region = cluster.regions[regionIndex];
    Set<Short> prefixes = HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(),
      HubSpotCellUtilities.MAX_CELL_COUNT);

    for (short prefix : prefixes) {
      updateStoreFilePerPrefixCosts(prefix);
    }
  }

  private void updateStoreFilePerPrefixCosts(short prefix) {
    Map<Integer, AtomicDouble> costsByServer = new HashMap<>();

    for (int regionIndex : regionsByPrefix.get(prefix)) {
      Collection<BalancerRegionLoad> regionLoadList = cluster.regionLoads[regionIndex];

      double regionCost = 0;
      if (regionLoadList != null) {
        regionCost = getRegionLoadCost(regionLoadList);
      }

      int serverHostingRegion = cluster.regionIndexToServerIndex[regionIndex];
      costsByServer.computeIfAbsent(serverHostingRegion, ignored -> new AtomicDouble())
        .addAndGet(regionCost);
    }

    DoubleArrayCost cost = new DoubleArrayCost();
    cost.prepare(costsByServer.size());
    cost.applyCostsChange(serverCosts -> {
      List<Integer> servers = new ArrayList<>(costsByServer.keySet());
      for (int i = 0; i < costsByServer.size(); i++) {
        serverCosts[i] = costsByServer.get(servers.get(i)).get();
      }
    });

    costPerPrefix.put(prefix, cost);
  }

  @Override protected double cost() {
    return costPerPrefix.values().stream().mapToDouble(DoubleArrayCost::cost).sum();
  }

  protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
    double cost = 0;
    if (regionLoadList.isEmpty()) {
      return cost;
    }

    // average observations if there are multiple
    for (BalancerRegionLoad rl : regionLoadList) {
      cost += getCostFromRl(rl);
    }
    return cost / regionLoadList.size();
  }

  protected abstract double getCostFromRl(BalancerRegionLoad rl);
}
