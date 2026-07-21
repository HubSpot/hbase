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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.NoEOFWALStreamReader;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALStreamReader;
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

  /**
   * Head-of-line blocking: Region A's stuck entry in the WAL reader prevents Region B (on the same
   * RS) from replicating. This reproduces the incident where moving a healthy region onto a stuck
   * RS caused the healthy region's replication to also stall.
   * <ol>
   * <li>Create table with 2 regions (split at row "m")</li>
   * <li>Write data to Region A (rows starting with "a")</li>
   * <li>Crash Region A's RS twice with no writes between → gap-1 bug on Region A</li>
   * <li>Move Region B onto the same RS as Region A</li>
   * <li>Write data to Region B (rows starting with "z") — these entries go into the same WAL as
   * Region A's stuck REGION_OPEN marker</li>
   * <li>Enable peer — Region B's entries are head-of-line blocked behind Region A's stuck
   * entry</li>
   * </ol>
   */
  @Test
  public void testRegionMovedOntoStuckRSIsAlsoStuck() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] splitKey = Bytes.toBytes("m");
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
          .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build(),
      new byte[][] { splitKey });
    UTIL.waitTableAvailable(tableName);

    RegionInfo regionA =
      UTIL.getConnection().getRegionLocator(tableName).getAllRegionLocations().stream()
        .filter(loc -> loc.getRegion().getStartKey().length == 0).findFirst().get().getRegion();
    RegionInfo regionB =
      UTIL.getConnection().getRegionLocator(tableName).getAllRegionLocations().stream()
        .filter(loc -> loc.getRegion().getStartKey().length > 0).findFirst().get().getRegion();

    HRegionServer rsForA = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .map(RegionServerThread::getRegionServer)
      .filter(rs -> rs.getRegion(regionA.getEncodedName()) != null).findFirst().get();
    HRegionServer rsForB = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .map(RegionServerThread::getRegionServer)
      .filter(rs -> rs.getRegion(regionB.getEncodedName()) != null).findFirst().get();

    // Ensure regions are on different RSes so Region B is not affected by the crashes
    if (rsForA.getServerName().equals(rsForB.getServerName())) {
      HRegionServer otherRS = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
        .map(RegionServerThread::getRegionServer)
        .filter(rs -> !rs.getServerName().equals(rsForA.getServerName())).findFirst().get();
      moveRegion(regionB, otherRS);
    }

    // Write data to Region A
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(
          new Put(Bytes.toBytes(String.format("a%04d", i))).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    // Crash Region A's RS twice with no writes between → gap-1 bug
    abortRSHostingRegion(tableName, regionA);
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .anyMatch(t -> t.getRegionServer().getRegion(regionA.getEncodedName()) != null));

    abortRSHostingRegion(tableName, regionA);
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .anyMatch(t -> t.getRegionServer().getRegion(regionA.getEncodedName()) != null));

    // Find the RS now hosting Region A and move Region B onto it
    HRegionServer stuckRS = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .map(RegionServerThread::getRegionServer)
      .filter(rs -> rs.getRegion(regionA.getEncodedName()) != null).findFirst().get();
    if (stuckRS.getRegion(regionB.getEncodedName()) == null) {
      moveRegion(regionB, stuckRS);
    }

    // Write data to Region B — these entries land in the same WAL as Region A's stuck entry
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(
          new Put(Bytes.toBytes(String.format("z%04d", i))).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    // With the bug, Region A's stuck entry blocks the WAL reader, so Region B's
    // entries are head-of-line blocked and never replicate.
    enablePeerAndWaitUntilReplicationDone(200);
    checkOrderPerRegion(200);
  }

  private void checkOrderPerRegion(int expectedEntries) throws IOException {
    try (WALStreamReader reader =
      NoEOFWALStreamReader.create(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      Map<String, Long> lastSeqIdByRegion = new HashMap<>();
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        String region = Bytes.toString(entry.getKey().getEncodedRegionName());
        long seqId = entry.getKey().getSequenceId();
        Long prev = lastSeqIdByRegion.get(region);
        assertTrue(
          "Sequence id go backwards for region " + region + " from " + prev + " to " + seqId,
          prev == null || seqId >= prev);
        lastSeqIdByRegion.put(region, seqId);
        count++;
      }
      assertEquals(expectedEntries, count);
    }
  }

  private void abortRSHostingRegion(TableName tableName, RegionInfo region) throws Exception {
    RegionServerThread rsThread = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .filter(t -> t.getRegionServer().getRegion(region.getEncodedName()) != null).findFirst()
      .orElseThrow(() -> new RuntimeException("No live RS hosting " + region.getEncodedName()));
    rsThread.getRegionServer().abort("crash for head-of-line blocking test");
    rsThread.join();
  }

  private void abortRSHostingRegion(TableName tableName) throws Exception {
    RegionServerThread rsThread = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .filter(t -> !t.getRegionServer().getRegions(tableName).isEmpty()).findFirst()
      .orElseThrow(() -> new RuntimeException("No live RS hosting " + tableName));
    rsThread.getRegionServer().abort("crash for HBASE-29499 test");
    rsThread.join();
  }
}
