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
package org.apache.hadoop.hbase.master.balancer;

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.buildStochasticLoadBalancer;
import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.createMockBalancerClusterState;
import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.partitionRegionsByTable;
import static org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.MAX_RUNNING_TIME_KEY;
import static org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.MIN_COST_NEED_BALANCE_KEY;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.replicas.ReplicaKeyCache;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test comparing balancing time with SlopFixingCandidateGenerator enabled vs disabled.
 * This simulates a "new empty server added to balanced cluster" scenario.
 */
@Category({ IntegrationTests.class, MasterTests.class })
public class TestSlopFixingCandidateGeneratorPerformance {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSlopFixingCandidateGeneratorPerformance.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSlopFixingCandidateGeneratorPerformance.class);

  private static final int NUM_SERVERS = 100;
  private static final int REGIONS_PER_SERVER = 100;
  private static final int NUM_TABLES = 10;
  private static final int NUM_REPLICAS = 3;
  private static final float TARGET_COST = 0.001f;
  private static final long MAX_RUNNING_TIME_MS = 30_000;
  private static final int MAX_BALANCER_RUNS = 50;

  @Test
  public void testBalancingPerformanceComparison() {
    LOG.info("=== Performance Test: SlopFixingCandidateGenerator Comparison ===");
    LOG.info("Cluster config: {} servers, ~{} regions/server, {} tables, {} replicas", NUM_SERVERS,
      REGIONS_PER_SERVER, NUM_TABLES, NUM_REPLICAS);

    long sfcgEnabledTime = runBalancerWithSfcg(true);
    long sfcgDisabledTime = runBalancerWithSfcg(false);

    LOG.info("=== Performance Results ===");
    LOG.info("With SFCG enabled:  {} ms", sfcgEnabledTime);
    LOG.info("With SFCG disabled: {} ms", sfcgDisabledTime);
    long diff = sfcgEnabledTime - sfcgDisabledTime;
    String comparison =
      diff > 0 ? "SFCG slower by " + diff + " ms" : "SFCG faster by " + (-diff) + " ms";
    LOG.info("Difference: {}", comparison);
  }

  private long runBalancerWithSfcg(boolean sfcgEnabled) {
    LOG.info("Starting balancer run with SFCG {}", sfcgEnabled ? "ENABLED" : "DISABLED");

    Map<ServerName, List<RegionInfo>> serverToRegions = createBalancedClusterThenAddEmptyServer();
    Configuration conf = createConfiguration();

    BalancerClusterState cluster = createMockBalancerClusterState(serverToRegions);
    StochasticLoadBalancer balancer = buildStochasticLoadBalancer(cluster, conf);

    if (!sfcgEnabled) {
      balancer.setSlopFixingCandidateGeneratorEnabled(false);
    }

    long startTime = System.currentTimeMillis();
    int balancerRuns = 0;
    int totalMoves = 0;

    while (balancerRuns < MAX_BALANCER_RUNS) {
      balancerRuns++;
      List<RegionPlan> regionPlans =
        balancer.balanceCluster(partitionRegionsByTable(serverToRegions));

      if (regionPlans == null || regionPlans.isEmpty()) {
        LOG.info("Balancer finished after {} runs with {} total moves", balancerRuns, totalMoves);
        break;
      }

      totalMoves += regionPlans.size();
      for (RegionPlan rp : regionPlans) {
        serverToRegions.get(rp.getSource()).remove(rp.getRegionInfo());
        serverToRegions.get(rp.getDestination()).add(rp.getRegionInfo());
      }

      cluster = createMockBalancerClusterState(serverToRegions);
      balancer = buildStochasticLoadBalancer(cluster, conf);
      if (!sfcgEnabled) {
        balancer.setSlopFixingCandidateGeneratorEnabled(false);
      }
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    LOG.info("SFCG {}: {} runs, {} moves, {} ms", sfcgEnabled ? "ENABLED" : "DISABLED",
      balancerRuns, totalMoves, elapsedTime);

    return elapsedTime;
  }

  private Map<ServerName, List<RegionInfo>> createBalancedClusterThenAddEmptyServer() {
    LOG.info("Creating imbalanced cluster and balancing it...");

    ServerName[] servers = new ServerName[NUM_SERVERS - 1];
    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();

    for (int i = 0; i < NUM_SERVERS - 1; i++) {
      servers[i] = ServerName.valueOf("server" + i, i, System.currentTimeMillis());
      serverToRegions.put(servers[i], new ArrayList<>());
    }

    int regionsPerTable = (REGIONS_PER_SERVER * (NUM_SERVERS - 1)) / NUM_TABLES;
    int regionIndex = 0;

    for (int t = 0; t < NUM_TABLES; t++) {
      TableName tableName = TableName.valueOf("table" + t);

      for (int r = 0; r < regionsPerTable; r++) {
        byte[] startKey = Bytes.toBytes(regionIndex);
        byte[] endKey = Bytes.toBytes(regionIndex + 1);

        for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
          RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey)
            .setEndKey(endKey).setReplicaId(replicaId).build();
          serverToRegions.get(servers[0]).add(regionInfo);
        }
        regionIndex++;
      }
    }

    int totalRegions = serverToRegions.values().stream().mapToInt(List::size).sum();
    LOG.info("Created imbalanced cluster with {} total regions all on server 0", totalRegions);

    balanceClusterToCompletion(serverToRegions);

    ServerName newServer =
      ServerName.valueOf("newServer", NUM_SERVERS - 1, System.currentTimeMillis());
    serverToRegions.put(newServer, new ArrayList<>());
    LOG.info("Added new empty server. Ready for performance test.");

    return serverToRegions;
  }

  private void balanceClusterToCompletion(Map<ServerName, List<RegionInfo>> serverToRegions) {
    Configuration conf = createConfiguration();
    BalancerClusterState cluster = createMockBalancerClusterState(serverToRegions);
    StochasticLoadBalancer balancer = buildStochasticLoadBalancer(cluster, conf);
    balancer.setSlopFixingCandidateGeneratorEnabled(false);

    int balancerRuns = 0;
    int totalMoves = 0;

    while (balancerRuns < MAX_BALANCER_RUNS) {
      balancerRuns++;
      List<RegionPlan> regionPlans =
        balancer.balanceCluster(partitionRegionsByTable(serverToRegions));

      if (regionPlans == null || regionPlans.isEmpty()) {
        break;
      }

      totalMoves += regionPlans.size();
      for (RegionPlan rp : regionPlans) {
        serverToRegions.get(rp.getSource()).remove(rp.getRegionInfo());
        serverToRegions.get(rp.getDestination()).add(rp.getRegionInfo());
      }

      cluster = createMockBalancerClusterState(serverToRegions);
      balancer = buildStochasticLoadBalancer(cluster, conf);
      balancer.setSlopFixingCandidateGeneratorEnabled(false);
    }

    LOG.info("Initial balancing complete: {} runs, {} moves", balancerRuns, totalMoves);
    logClusterDistribution(serverToRegions);
  }

  private void logClusterDistribution(Map<ServerName, List<RegionInfo>> serverToRegions) {
    int min = Integer.MAX_VALUE;
    int max = 0;
    int total = 0;
    for (List<RegionInfo> regions : serverToRegions.values()) {
      int size = regions.size();
      min = Math.min(min, size);
      max = Math.max(max, size);
      total += size;
    }
    double avg = (double) total / serverToRegions.size();
    LOG.info("Cluster distribution: min={}, max={}, avg={:.1f}, total={}", min, max, avg, total);
  }

  private Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);
    conf.setLong(MAX_RUNNING_TIME_KEY, MAX_RUNNING_TIME_MS);
    conf.setFloat(MIN_COST_NEED_BALANCE_KEY, TARGET_COST);
    conf.setBoolean(BalancerConditionals.DISTRIBUTE_REPLICAS_KEY, true);
    conf.setBoolean(ReplicaKeyCache.CACHE_REPLICA_KEYS_KEY, true);
    conf.setInt(ReplicaKeyCache.REPLICA_KEY_CACHE_SIZE_KEY, Integer.MAX_VALUE);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, 0.05f);
    conf.setLong("hbase.master.balancer.stochastic.regionReplicaRackCostKey", 0);
    conf.setLong("hbase.master.balancer.stochastic.regionReplicaHostCostKey", 0);
    return conf;
  }
}
