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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Confirm that we will batch region reopens when reopening all table regions. This can avoid the
 * pain associated with reopening too many regions at once.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureBatching {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureBatching.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int BACKOFF_MILLIS_PER_RS = 0;
  private static final int REOPEN_BATCH_SIZE = 1;

  private static TableName TABLE_NAME = TableName.valueOf("Batching");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(1);
    UTIL.createMultiRegionTable(TABLE_NAME, CF);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegionBatching() throws IOException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(2 <= regions.size());
    Set<StuckRegion> stuckRegions =
      regions.stream().map(r -> stickRegion(am, procExec, r)).collect(Collectors.toSet());
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, BACKOFF_MILLIS_PER_RS, REOPEN_BATCH_SIZE);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);
    confirmBatchSize(REOPEN_BATCH_SIZE, stuckRegions, proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);
  }

  @Test
  public void testNoRegionBatching() throws IOException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(2 <= regions.size());
    Set<StuckRegion> stuckRegions =
      regions.stream().map(r -> stickRegion(am, procExec, r)).collect(Collectors.toSet());
    ReopenTableRegionsProcedure proc = new ReopenTableRegionsProcedure(TABLE_NAME);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);
    confirmBatchSize(regions.size(), stuckRegions, proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);
  }

  private void confirmBatchSize(int expectedBatchSize, Set<StuckRegion> stuckRegions,
    ReopenTableRegionsProcedure proc) {
    while (true) {
      List<HRegionLocation> currentRegionBatch = new ArrayList<>(proc.currentRegionBatch);
      if (currentRegionBatch.isEmpty()) {
        continue;
      }
      stuckRegions.forEach(this::unstickRegion);
      assertEquals(expectedBatchSize, currentRegionBatch.size());
      break;
    }
  }

  static class StuckRegion {
    final TransitRegionStateProcedure trsp;
    final RegionStateNode regionNode;
    final long openSeqNum;

    public StuckRegion(TransitRegionStateProcedure trsp, RegionStateNode regionNode,
      long openSeqNum) {
      this.trsp = trsp;
      this.regionNode = regionNode;
      this.openSeqNum = openSeqNum;
    }
  }

  private StuckRegion stickRegion(AssignmentManager am,
    ProcedureExecutor<MasterProcedureEnv> procExec, RegionInfo regionInfo) {
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(regionInfo);
    TransitRegionStateProcedure trsp =
      TransitRegionStateProcedure.unassign(procExec.getEnvironment(), regionInfo);
    regionNode.lock();
    long openSeqNum;
    try {
      openSeqNum = regionNode.getOpenSeqNum();
      regionNode.setState(State.OPENING);
      regionNode.setOpenSeqNum(-1L);
      regionNode.setProcedure(trsp);
    } finally {
      regionNode.unlock();
    }
    return new StuckRegion(trsp, regionNode, openSeqNum);
  }

  private void unstickRegion(StuckRegion stuckRegion) {
    stuckRegion.regionNode.lock();
    try {
      stuckRegion.regionNode.setState(State.OPEN);
      stuckRegion.regionNode.setOpenSeqNum(stuckRegion.openSeqNum);
      stuckRegion.regionNode.unsetProcedure(stuckRegion.trsp);
    } finally {
      stuckRegion.regionNode.unlock();
    }
  }
}
