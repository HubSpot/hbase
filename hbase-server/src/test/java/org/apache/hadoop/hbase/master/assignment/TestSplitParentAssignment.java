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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that split parent regions cannot be assigned via any path, regardless of whether the
 * in-memory state reflects SPLIT or CLOSED (as happens after a master failover, where meta only
 * persists CLOSED and the SPLIT state is lost). Regression test for HBASE-30353.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestSplitParentAssignment extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitParentAssignment.class);

  /**
   * Simulates the post-failover condition: split parent loaded from meta with state=CLOSED because
   * MetaTableAccessor.splitRegion never writes SPLIT to info:state. Assigning this region via the
   * normal path (and the HBCK2 force path) must be rejected.
   */
  @Test
  public void testSplitParentCannotBeAssignedWhenStateIsClosedAfterFailover() throws Exception {
    RegionInfo splitParent = RegionInfoBuilder.newBuilder(TableName.valueOf("test-table"))
      .setSplit(true).setOffline(true).build();

    // Simulate how loadMeta reconstructs the node after failover: state=CLOSED, not SPLIT.
    RegionStateNode rsn = am.getRegionStates().getOrCreateRegionStateNode(splitParent);
    rsn.setState(RegionState.State.CLOSED);

    // Non-override path (normal assign and HBCK2 without --force): must throw.
    try {
      am.assign(splitParent);
      fail("Expected DoNotRetryIOException for split parent assign");
    } catch (DoNotRetryIOException e) {
      // expected — message should mention split parent
      assertTrue(e.getMessage(), e.getMessage().contains("split parent"));
    }

    // Override path (HBCK2 with --force): createOneAssignProcedure swallows the exception
    // and returns null on rejection.
    assertNull(am.createOneAssignProcedure(splitParent, true));
  }

  /**
   * Verifies that a split parent with state=SPLIT (the healthy-master case) is also rejected.
   * Belt-and-suspenders: confirms the guard works regardless of which state value is in memory.
   */
  @Test
  public void testSplitParentCannotBeAssignedWhenStateIsSplit() throws Exception {
    RegionInfo splitParent = RegionInfoBuilder.newBuilder(TableName.valueOf("test-table-2"))
      .setSplit(true).setOffline(true).build();

    RegionStateNode rsn = am.getRegionStates().getOrCreateRegionStateNode(splitParent);
    rsn.setState(RegionState.State.SPLIT);

    try {
      am.assign(splitParent);
      fail("Expected DoNotRetryIOException for split parent assign");
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("split parent"));
    }

    assertNull(am.createOneAssignProcedure(splitParent, true));
  }
}
