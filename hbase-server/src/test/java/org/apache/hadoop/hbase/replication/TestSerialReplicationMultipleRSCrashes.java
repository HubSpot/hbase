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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Regression tests for HBASE-29499: serial replication stuck when a WAL entry's seqId exactly
 * matches a replication barrier value.
 * <p>
 * When an RS is SIGKILL'ed and the region reopens on a new RS, a REGION_OPEN marker is written to
 * the WAL with a seqId close to the new barrier value. If the RS is killed again, the new barrier
 * can exactly match the REGION_OPEN marker's seqId from the previous incarnation. This causes
 * {@code SerialReplicationChecker.canPush()} to check the wrong barrier endpoint, permanently
 * blocking replication for the affected region.
 * <p>
 * The blocked entry then causes head-of-line blocking: all subsequent entries for that region are
 * also blocked because their "previous range" can never be marked as finished.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestSerialReplicationMultipleRSCrashes extends SerialReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialReplicationMultipleRSCrashes.class);

  @Before
  public void setUp() throws Exception {
    setupWALWriter();
    addPeer(false);
    while (UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() < 3) {
      UTIL.getMiniHBaseCluster().startRegionServer();
    }
  }

  /**
   * Two consecutive RS crashes with data writes between them. After two crashes, the WAL reader
   * must process entries from the old RS's WAL (via RS_CLAIM_REPLICATION_QUEUE) including
   * REGION_OPEN markers whose seqIds can match barrier values. Replication must complete for all
   * entries.
   */
  @Test
  public void testTwoConsecutiveRSCrashes() throws Exception {
    TableName tableName = createTable();

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 200; i < 300; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    enablePeerAndWaitUntilReplicationDone(300);
    checkOrder(300);
  }

  /**
   * Two consecutive RS crashes with NO data writes between them. This closely mimics the scenario
   * from the HBASE-29499 report where the only WAL entries between the first and second crash are
   * internal markers (REGION_OPEN, COMPACTION). When the second crash happens, the new barrier
   * value is likely to match the REGION_OPEN marker's seqId from the first restart, because no user
   * writes advanced the region's sequence counter beyond the open marker.
   */
  @Test
  public void testTwoConsecutiveRSCrashesNoWritesBetween() throws Exception {
    TableName tableName = createTable();

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    enablePeerAndWaitUntilReplicationDone(200);
    checkOrder(200);
  }

  /**
   * Three consecutive RS crashes to increase the number of barriers and the probability that a WAL
   * entry's seqId matches one of them. After three crashes, the replication system must claim and
   * process queues from two dead RS incarnations plus the current RS's own queue.
   */
  @Test
  public void testThreeConsecutiveRSCrashes() throws Exception {
    TableName tableName = createTable();

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 50; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 50; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() >= 2);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 150; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    abortRSHostingRegion(tableName);
    UTIL.waitTableAvailable(tableName);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 150; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    enablePeerAndWaitUntilReplicationDone(200);
    checkOrder(200);
  }

  private void abortRSHostingRegion(TableName tableName) throws Exception {
    RegionServerThread rsThread = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .filter(t -> !t.getRegionServer().getRegions(tableName).isEmpty()).findFirst()
      .orElseThrow(() -> new RuntimeException("No live RS hosting " + tableName));
    rsThread.getRegionServer().abort("crash for HBASE-29499 test");
    rsThread.join();
  }
}
