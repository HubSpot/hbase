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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests that WAL files from offline/inactive RegionServers are handled correctly during backup.
 * Specifically verifies that WALs from an offline RS are:
 * <ol>
 * <li>Backed up once in the first backup after the RS goes offline</li>
 * <li>NOT re-backed up in subsequent backups</li>
 * </ol>
 */
@Category(LargeTests.class)
public class TestBackupOfflineRS extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupOfflineRS.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupOfflineRS.class);

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setInt("hbase.regionserver.info.port", -1);
    conf1.setInt(HMaster.HBASE_MASTER_CLEANER_INTERVAL, Integer.MAX_VALUE);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.waitTableAvailable(table1);
  }

  private static Runnable afterSnapshotHook;

  /** Full backup client that runs {@link #afterSnapshotHook} once the table snapshots exist. */
  public static class FullTableBackupClientWithHook extends FullTableBackupClient {
    @Override
    protected void snapshotCopy(BackupInfo backupInfo) throws Exception {
      afterSnapshotHook.run();
      super.snapshotCopy(backupInfo);
    }
  }

  private static void stopRegionServerAndWait(ServerName serverName) throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    HMaster master = cluster.getMaster();
    cluster.stopRegionServer(serverName);
    cluster.waitForRegionServerToStop(serverName, 60_000);
    TEST_UTIL.waitFor(60_000,
      () -> master.getProcedures().stream().filter(ServerCrashProcedure.class::isInstance)
        .map(ServerCrashProcedure.class::cast)
        .anyMatch(scp -> scp.getServerName().equals(serverName) && scp.isFinished()));
    TEST_UTIL.waitUntilNoRegionsInTransition(60_000);
  }

  private static void restore(String backupId, TableName sourceTable, TableName restoredTable)
    throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupId, false,
        new TableName[] { sourceTable }, new TableName[] { restoredTable }, true));
    }
  }

  private static void restoreAndAssertAllRowsPresent(String backupId, String restoredTableName,
    String message) throws Exception {
    TableName restoredTable = TableName.valueOf(restoredTableName);
    restore(backupId, table1, restoredTable);
    assertEquals(message, TEST_UTIL.countRows(table1), TEST_UTIL.countRows(restoredTable));
  }

  private static Long boundary(BackupSystemTable sysTable, String host) throws IOException {
    return sysTable.readLogTimestampMap(BACKUP_ROOT_DIR).get(table1).get(host);
  }

  private static void moveRegionAndWait(TableName table, HRegionServer destination)
    throws Exception {
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      RegionInfo region = admin.getRegions(table).get(0);
      admin.move(region.getEncodedNameAsBytes(), destination.getServerName());
    }
    TEST_UTIL.waitFor(60_000, () -> !destination.getRegions(table).isEmpty());
    TEST_UTIL.waitUntilAllRegionsAssigned(table);
  }

  /**
   * Tests that when a full backup is taken while an RS is offline (with WALs in oldlogs), the
   * offline host's timestamps are recorded so subsequent incremental backups don't reinclude those
   * WALs.
   */
  @Test
  public void testBackupWithOfflineRS() throws Exception {
    LOG.info("Starting testBackupWithOfflineRS");

    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    HRegionServer rsBeforeStop = cluster.startRegionServerAndWait(10000).getRegionServer();
    moveRegionAndWait(table1, rsBeforeStop);

    LOG.info("Inserting data to generate WAL entries");
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 2, 100);
    }

    String offlineHost =
      rsBeforeStop.getServerName().getHostname() + ":" + rsBeforeStop.getServerName().getPort();
    LOG.info("Stopping RS: {}", offlineHost);

    stopRegionServerAndWait(rsBeforeStop.getServerName());

    LOG.info("Taking full backup (with offline RS WALs in oldlogs)");
    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      LOG.info("RS timestamps after full backup: {}", rsTimestamps);

      Long tsAfterFullBackup = rsTimestamps.get(offlineHost);
      assertNotNull("Offline host should have timestamp recorded in trslm after full backup",
        tsAfterFullBackup);

      LOG.info("Taking incremental backup (should NOT include offline RS WALs)");
      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

      assertEquals("Incremental backup moved the boundary of the offline host", tsAfterFullBackup,
        boundary(sysTable, offlineHost));
    }
  }

  /**
   * Tests that WALs written to an RS after a full backup are correctly included in the subsequent
   * incremental backup, even if that RS has gone offline before the incremental runs.
   */
  @Test
  public void testRSGoesOfflineAfterFullBackupBeforeIncremental() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    HRegionServer rsBeforeStop = cluster.startRegionServerAndWait(10000).getRegionServer();

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 3, 50);
    }

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    moveRegionAndWait(table1, rsBeforeStop);
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 4, 50);
    }

    rsBeforeStop.getWalRoller().requestRollAll();
    rsBeforeStop.getWalRoller().waitUntilWalRollFinished();
    String offlineHost =
      rsBeforeStop.getServerName().getHostname() + ":" + rsBeforeStop.getServerName().getPort();

    stopRegionServerAndWait(rsBeforeStop.getServerName());

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

    restoreAndAssertAllRowsPresent(incrBackupId, "table1_rs_offline_after_full_backup",
      "Restored table should contain the rows written to the RS that went offline");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Long boundaryAfterIncr1 = boundary(sysTable, offlineHost);
      assertNotNull(
        "Offline RS should have a timestamp boundary after the incremental backed up its WALs",
        boundaryAfterIncr1);
      Long staleLogRoll =
        sysTable.readRegionServerLastLogRollResult(BACKUP_ROOT_DIR).get(offlineHost);
      String message = "Incremental backup moved the boundary of the offline RS, which would back "
        + "up its WALs again (last log roll = " + staleLogRoll + ")";

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue("Second incremental backup should succeed", checkSucceeded(incrBackupId2));
      assertEquals(message, boundaryAfterIncr1, boundary(sysTable, offlineHost));

      String incrBackupId3 = incrementalTableBackup(tables);
      assertTrue("Third incremental backup should succeed", checkSucceeded(incrBackupId3));
      assertEquals(message, boundaryAfterIncr1, boundary(sysTable, offlineHost));

      String incrBackupId4 = incrementalTableBackup(tables);
      assertTrue("Fourth incremental backup should succeed", checkSucceeded(incrBackupId4));
      assertEquals(message, boundaryAfterIncr1, boundary(sysTable, offlineHost));
    }
  }

  /**
   * Tests that a brand-new RS that comes online and goes offline before any backup correctly has
   * its WALs covered by the full backup.
   */
  @Test
  public void testTransientRSBeforeFullBackup() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    HRegionServer transientRS = cluster.startRegionServerAndWait(10000).getRegionServer();
    moveRegionAndWait(table1, transientRS);
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 5, 50);
    }
    transientRS.getWalRoller().requestRollAll();
    transientRS.getWalRoller().waitUntilWalRollFinished();
    String transientHost =
      transientRS.getServerName().getHostname() + ":" + transientRS.getServerName().getPort();

    stopRegionServerAndWait(transientRS.getServerName());

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Long boundaryAfterFullBackup = boundary(sysTable, transientHost);
      assertNotNull(
        "Transient RS that went offline before full backup should have its WAL boundary recorded",
        boundaryAfterFullBackup);

      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup after transient RS should succeed",
        checkSucceeded(incrBackupId));

      assertEquals("Incremental backup moved the boundary of the transient RS",
        boundaryAfterFullBackup, boundary(sysTable, transientHost));
    }
  }

  /**
   * Tests that WALs from an RS that comes online and goes offline between a full backup and an
   * incremental backup are correctly included in the incremental backup and not re-included in
   * subsequent incremental backups.
   */
  @Test
  public void testTransientRSAfterFullBackupBeforeIncremental() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 6, 50);
    }

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    HRegionServer transientRS = cluster.startRegionServerAndWait(10000).getRegionServer();
    moveRegionAndWait(table1, transientRS);
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 7, 50);
    }
    transientRS.getWalRoller().requestRollAll();
    transientRS.getWalRoller().waitUntilWalRollFinished();
    String transientHost =
      transientRS.getServerName().getHostname() + ":" + transientRS.getServerName().getPort();

    stopRegionServerAndWait(transientRS.getServerName());

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

    restoreAndAssertAllRowsPresent(incrBackupId, "table1_transient_rs_after_full_backup",
      "Restored table should contain the rows written to the transient RS");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Long boundaryAfterIncr1 = boundary(sysTable, transientHost);
      assertNotNull(
        "Transient RS should have a timestamp boundary after the incremental backed up its WALs",
        boundaryAfterIncr1);

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue("Second incremental backup should succeed", checkSucceeded(incrBackupId2));

      assertEquals("Second incremental backup moved the boundary of the transient RS",
        boundaryAfterIncr1, boundary(sysTable, transientHost));
    }
  }

  /**
   * Tests that edits written during a full backup, after its log roll and table snapshot, to an RS
   * that started after that log roll, are included in the next incremental backup.
   */
  @Test
  public void testRSStartedDuringFullBackup() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    afterSnapshotHook = () -> {
      try {
        HRegionServer newRS = cluster.startRegionServerAndWait(10000).getRegionServer();
        moveRegionAndWait(table1, newRS);
        try (Connection conn = ConnectionFactory.createConnection(conf1)) {
          insertIntoTable(conn, table1, famName, 8, 50).close();
        }
        stopRegionServerAndWait(newRS.getServerName());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientWithHook.class.getName());
    String fullBackupId;
    try {
      fullBackupId = fullTableBackup(tables);
    } finally {
      conf1.unset(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS);
    }
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

    restoreAndAssertAllRowsPresent(incrBackupId, "table1_rs_started_during_full_backup",
      "Restored table should contain all rows, including those written to the new RS");
  }

  /**
   * Tests that a row deleted before a full backup is not brought back by the WALs of a region
   * server that went offline between two backups. The row and its delete marker are compacted away
   * before the full backup, so only the offline RS's old WAL, which still holds the put, could
   * bring it back.
   */
  @Test
  public void testDeletedRowIsNotResurrectedByOfflineRSWALs() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);
    byte[] deletedRow = Bytes.toBytes("row-deleted");

    HRegionServer offlineRS = cluster.startRegionServerAndWait(10000).getRegionServer();
    HRegionServer liveRS = cluster.startRegionServerAndWait(10000).getRegionServer();

    try (Table table = TEST_UTIL.getConnection().getTable(table1)) {
      moveRegionAndWait(table1, offlineRS);

      String firstFullBackupId = fullTableBackup(tables);
      assertTrue("First full backup should succeed", checkSucceeded(firstFullBackupId));

      for (int i = 0; i < 10; i++) {
        table.put(new Put(Bytes.toBytes("row-kept-" + i)).addColumn(famName, qualName,
          Bytes.toBytes("value")));
      }
      table.put(new Put(deletedRow).addColumn(famName, qualName, Bytes.toBytes("value")));
      for (HRegion region : offlineRS.getRegions(table1)) {
        region.flush(true);
      }

      moveRegionAndWait(table1, liveRS);

      table.delete(new Delete(deletedRow));
      for (HRegion region : liveRS.getRegions(table1)) {
        region.flush(true);
        region.compact(true);
      }

      assertTrue("Deleted row should be gone from the source table",
        table.get(new Get(deletedRow)).isEmpty());
      Scan rawScan = new Scan().withStartRow(deletedRow).withStopRow(deletedRow, true).setRaw(true);
      try (ResultScanner scanner = table.getScanner(rawScan)) {
        assertNull("Major compaction should have purged the deleted row and its delete marker",
          scanner.next());
      }

      stopRegionServerAndWait(offlineRS.getServerName());

      String secondFullBackupId = fullTableBackup(tables);
      assertTrue("Second full backup should succeed", checkSucceeded(secondFullBackupId));
      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

      TableName restoredTable = TableName.valueOf("table1_deleted_row_not_resurrected");
      restore(incrBackupId, table1, restoredTable);
      try (Table restored = TEST_UTIL.getConnection().getTable(restoredTable)) {
        assertTrue("Deleted row was brought back by the WALs of the offline RS",
          restored.get(new Get(deletedRow)).isEmpty());
      }
      assertEquals("Restored table should match the source table", TEST_UTIL.countRows(table1),
        TEST_UTIL.countRows(restoredTable));
    }
  }
}
