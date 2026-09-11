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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regression test for HBASE-30353 / HubSpot #2627: a split parent region must never be re-opened
 * after master failover.
 * <p>
 * Root cause: {@code MetaTableAccessor.splitRegion} writes {@code split=true, offline=true} into
 * {@code info:regioninfo} but never writes SPLIT into {@code info:state} — that cell stays as
 * CLOSED from the pre-split unassign step. After failover, {@code loadMeta} reconstructs the
 * RegionStateNode with {@code state=CLOSED}, so {@code preTransitCheck} (which only checks state,
 * not {@code regionInfo.isSplit()}) accepts the parent for assignment.
 * <p>
 * Fix: {@code RegionStateNode.checkNotRetired()} throws {@link DoNotRetryRegionException} when
 * {@code isSplit()} is true, and {@code preTransitCheck} calls it before allowing any external
 * assign to proceed.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSplitParentAssignment {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitParentAssignment.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitParentAssignment.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String CF = "cf";
  private static final int ROW_COUNT = 60;
  private static final int START_ROW = 11;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws Exception {
    // Prevent CatalogJanitor from GC-ing the split parent before the test can use it.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    // Prevent compaction: if daughters compact away reference files, the parent becomes
    // GC-eligible even with CatalogJanitor disabled.
    for (int i = 0; i < UTIL.getHBaseCluster().getLiveRegionServerThreads().size(); i++) {
      UTIL.getHBaseCluster().getRegionServer(i).getCompactSplitThread().switchCompaction(false);
    }
  }

  @After
  public void tearDown() throws Exception {
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(true);
    for (TableDescriptor htd : UTIL.getAdmin().listTableDescriptors()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }

  /**
   * Reproduces HBASE-30353: simulates the post-failover state where a new master reads the split
   * parent from meta ({@code regionInfo.isSplit()=true}, {@code state=CLOSED}), then asserts that
   * {@code assign()} rejects the parent.
   * <p>
   * After a split, {@code markRegionAsSplit} intentionally does NOT update the in-memory
   * {@code RegionStateNode.regionInfo} — only meta gets {@code split=true}. After failover,
   * {@code loadMeta} creates a FRESH {@code RegionStateNode} from the meta row, where
   * {@code regionInfo.isSplit()=true} but {@code state=CLOSED} (because {@code info:state} is never
   * written to SPLIT). This test reconstructs that exact in-memory state by removing the existing
   * node and inserting a fresh one built from a {@code split=true} {@link RegionInfo}.
   */
  @Test
  public void testAssignSplitParentIsRejected() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(procExec, tableName, null, CF);
    insertData(UTIL, tableName, ROW_COUNT, START_ROW, CF);

    int splitRowNum = START_ROW + ROW_COUNT / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    RegionInfo parentInfo = regions[0];

    // Simulate post-failover: loadMeta creates a fresh RegionStateNode from meta, where
    // regionInfo.isSplit()=true and state=CLOSED. Reproduce by removing the existing node
    // (which has state=SPLIT but regionInfo.isSplit()=false — markRegionAsSplit never updates
    // the in-memory RegionInfo) and re-creating it from a split=true RegionInfo.
    am.getRegionStates().deleteRegion(parentInfo);
    RegionInfo splitParentInfo = RegionInfoBuilder.newBuilder(parentInfo.getTable())
      .setStartKey(parentInfo.getStartKey()).setEndKey(parentInfo.getEndKey())
      .setRegionId(parentInfo.getRegionId()).setSplit(true).setOffline(true).build();
    RegionStateNode freshRsn = am.getRegionStates().getOrCreateRegionStateNode(splitParentInfo);
    freshRsn.setState(RegionState.State.CLOSED);

    assertTrue("Precondition: regionInfo.isSplit() must be true on freshRsn",
      freshRsn.getRegionInfo().isSplit());
    assertTrue("Precondition: isSplit() must return true", freshRsn.isSplit());
    assertEquals("Precondition: state must be CLOSED to reproduce the bug",
      RegionState.State.CLOSED, freshRsn.getState());

    // Without the fix: preTransitCheck sees CLOSED ∈ {CLOSED, OFFLINE} and passes — bug.
    // With the fix: checkNotRetired() throws DoNotRetryRegionException before any procedure runs.
    try {
      am.assign(splitParentInfo);
      fail("Expected DoNotRetryRegionException: split parent must not be assignable");
    } catch (DoNotRetryRegionException expected) {
      assertTrue("Exception message must identify the region",
        expected.getMessage().contains(splitParentInfo.getEncodedName()));
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
