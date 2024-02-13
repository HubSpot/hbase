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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncDecommissionAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncDecommissionAdminApi.class);

  @Test
  public void testAsyncDecommissionRegionServers() throws Exception {
    admin.balancerSwitch(false, true);
    List<ServerName> decommissionedRegionServers = admin.listDecommissionedRegionServers().get();
    assertTrue(decommissionedRegionServers.isEmpty());

    TEST_UTIL.createMultiRegionTable(tableName, FAMILY, 4);

    ArrayList<ServerName> clusterRegionServers = new ArrayList<>(admin
      .getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).get().getLiveServerMetrics().keySet());

    assertEquals(TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size(),
      clusterRegionServers.size());

    HashMap<ServerName, List<RegionInfo>> serversToDecommission = new HashMap<>();
    // Get a server that has regions. We will decommission one of the servers,
    // leaving one online.
    int i;
    for (i = 0; i < clusterRegionServers.size(); i++) {
      List<RegionInfo> regionsOnServer = admin.getRegions(clusterRegionServers.get(i)).get();
      if (!regionsOnServer.isEmpty()) {
        serversToDecommission.put(clusterRegionServers.get(i), regionsOnServer);
        break;
      }
    }

    clusterRegionServers.remove(i);
    ServerName remainingServer = clusterRegionServers.get(0);

    // Decommission
    admin.decommissionRegionServers(new ArrayList<ServerName>(serversToDecommission.keySet()), true)
      .get();
    assertEquals(1, admin.listDecommissionedRegionServers().get().size());

    // Verify the regions have been off the decommissioned servers, all on the remaining server.
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Maybe the TRSP is still not finished at master side, since the reportRegionTransition just
    // updates the procedure store, and we still need to wake up the procedure and execute it in the
    // procedure executor, which is asynchronous
    TEST_UTIL.waitUntilNoRegionsInTransition(10000);

    // Recommission and load regions
    for (ServerName server : serversToDecommission.keySet()) {
      List<byte[]> encodedRegionNames = serversToDecommission.get(server).stream()
        .map(RegionInfo::getEncodedNameAsBytes).collect(Collectors.toList());
      admin.recommissionRegionServer(server, encodedRegionNames).get();
    }
    assertTrue(admin.listDecommissionedRegionServers().get().isEmpty());
    // Verify the regions have been moved to the recommissioned servers
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, server, 10000);
      }
    }
  }

  @Test
  public void testAsyncDecommissionRegionServersByHostNamesPermanently() throws Exception {
    admin.balancerSwitch(false, true);
    List<ServerName> decommissionedRegionServers = admin.listDecommissionedRegionServers().get();
    assertTrue(decommissionedRegionServers.isEmpty());

    TEST_UTIL.createMultiRegionTable(tableName, FAMILY, 4);

    ArrayList<ServerName> clusterRegionServers = new ArrayList<>(admin
      .getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).get().getLiveServerMetrics().keySet());

    assertEquals(TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size(),
      clusterRegionServers.size());

    HashMap<ServerName, List<RegionInfo>> serversToDecommission = new HashMap<>();
    // Get a server that has regions. We will decommission one of the servers,
    // leaving one online.
    int i;
    for (i = 0; i < clusterRegionServers.size(); i++) {
      List<RegionInfo> regionsOnServer = admin.getRegions(clusterRegionServers.get(i)).get();
      if (!regionsOnServer.isEmpty()) {
        serversToDecommission.put(clusterRegionServers.get(i), regionsOnServer);
        break;
      }
    }

    clusterRegionServers.remove(i);
    ServerName remainingServer = clusterRegionServers.get(0);

    // Decommission the server permanently, setting the `matchHostNameOnly` argument to `true`
    boolean matchHostNameOnly = true;
    admin.decommissionRegionServers(new ArrayList<>(serversToDecommission.keySet()), true,
      matchHostNameOnly).get();
    assertEquals(1, admin.listDecommissionedRegionServers().get().size());

    // Verify the regions have been off the decommissioned servers, all on the remaining server.
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Maybe the TRSP is still not finished at master side, since the reportRegionTransition just
    // updates the procedure store, and we still need to wake up the procedure and execute it in the
    // procedure executor, which is asynchronous
    TEST_UTIL.waitUntilNoRegionsInTransition(10000);

    // Try to recommission the server and assert that the server is still decommissioned
    recommissionRegionServers(serversToDecommission);
    assertEquals(1, admin.listDecommissionedRegionServers().get().size());

    // Verify that the regions still belong to the remainingServer and not the decommissioned ones
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Clean-up ZooKeeper's state and recommission all servers for the next parameterized test run
    removeServersBinaryData(serversToDecommission.keySet());
    recommissionRegionServers(serversToDecommission);
  }

  private void
    recommissionRegionServers(HashMap<ServerName, List<RegionInfo>> decommissionedServers)
      throws ExecutionException, InterruptedException {
    for (ServerName server : decommissionedServers.keySet()) {
      List<byte[]> encodedRegionNames = decommissionedServers.get(server).stream()
        .map(RegionInfo::getEncodedNameAsBytes).collect(Collectors.toList());
      admin.recommissionRegionServer(server, encodedRegionNames).get();
    }
  }

  private void removeServersBinaryData(Set<ServerName> decommissionedServers) throws IOException {
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    for (ServerName serverName : decommissionedServers) {
      String znodePath =
        ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode, serverName.getServerName());
      byte[] newData = ZooKeeperProtos.DrainedZNodeServerData.newBuilder()
        .setMatchHostNameOnly(false).build().toByteArray();
      try {
        ZKUtil.setData(zkw, znodePath, newData);
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
