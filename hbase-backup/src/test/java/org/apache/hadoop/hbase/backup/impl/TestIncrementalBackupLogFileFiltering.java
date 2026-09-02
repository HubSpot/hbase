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
package org.apache.hadoop.hbase.backup.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduction of the incremental-backup WAL filtering data loss, reconstructed from the
 * ajax-hb2-a-prod incident. It drives {@code IncrementalBackupManager.getLogFilesForNewBackup}
 * directly against a crafted WAL layout on the local filesystem (no mini-cluster) so it can assert
 * exactly which WAL files the incremental backup would include.
 * <p>
 * The incident condition — and why it is reachable by normal operation:
 * <ol>
 * <li>A RegionServer's per-server backup boundary ({@code rslogts}, read via
 * {@code readRegionServerLastLogRollResult}) is only advanced when it participates in the backup
 * log-roll, which runs on <em>live</em> servers only (LogRollBackupSubprocedure). The table is
 * persistent, so a decommissioned host keeps its last value forever.</li>
 * <li>The host keeps serving writes after that last roll, so it accumulates WALs whose timestamp is
 * NEWER than its recorded boundary.</li>
 * <li>On graceful decommission those WALs are archived to {@code oldWALs/}.</li>
 * <li>The next incremental backup rolls logs on live servers only, so the dead host's boundary
 * stays frozen: it is present in {@code newestTimestamps} but with a stale value below its
 * un-backed-up WALs.</li>
 * </ol>
 * In {@code getLogFilesForNewBackup}'s {@code .oldlogs} loop the file is first added to the result
 * (its ts exceeds the previous boundary) and then re-added to {@code newestLogs} because
 * {@code currentLogTS > newTimestamp}, so {@code resultLogFiles.removeAll(newestLogs)} drops it —
 * even though a dead host's archived WAL is final and will never be captured by a later backup.
 * <p>
 * Numbers are the real ajax values for na1-brainy-ratty-curtain in the property-scan-index-1 root
 * (from the backup:system dump): frozen boundary Aug 23 17:24, lost WAL Aug 24 15:05.
 */
@Tag(SmallTests.TAG)
public class TestIncrementalBackupLogFileFiltering {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupLogFileFiltering.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // Real values decoded from backup-system-ajax.txt (property-scan-index-1 root).
  private static final long DEAD_FROZEN_BOUNDARY = 1787505892716L; // brainy rslogts, Aug 23 17:24
  private static final long DEAD_LOST_WAL_TS = 1787583924745L; // Aug 24 15:05, holds the lost cell
  private static final long ACTIVE_PREV_BOUNDARY = 1787505000000L;
  private static final long ACTIVE_FRESH_BOUNDARY = 1787585400000L; // advanced by this cycle's roll
  private static final long ACTIVE_WAL_TS = 1787583009494L; // in range (prev < ts <= fresh)

  @Test
  public void testDecommissionedHostFrozenBoundaryWalIsIncludedInBackup() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    Path walRoot = TEST_UTIL.getDataTestDir("walFilteringRepro");
    conf.set(HConstants.HBASE_DIR, walRoot.toString());
    conf.unset(CommonFSUtils.HBASE_WAL_DIR);

    FileSystem fs = walRoot.getFileSystem(conf);
    Path oldLogDir = new Path(walRoot, HConstants.HREGION_OLDLOGDIR_NAME);
    fs.mkdirs(new Path(walRoot, HConstants.HREGION_LOGDIR_NAME));
    fs.mkdirs(oldLogDir);

    // Decommissioned host: only WAL is an archived one newer than its frozen boundary.
    ServerName deadHost = ServerName.valueOf("na1-brainy-ratty-curtain.iad02.hubspot-networks.net",
      60020, 1786305484662L);
    Path deadOldWal = archivedWal(oldLogDir, deadHost, DEAD_LOST_WAL_TS);
    fs.create(deadOldWal).close();
    String deadKey = deadHost.getAddress().toString();

    // Still-active host: an in-range archived WAL that must always be backed up (positive control).
    ServerName activeHost =
      ServerName.valueOf("na1-active-live-host.iad02.hubspot-networks.net", 60020, 1786305484662L);
    Path activeOldWal = archivedWal(oldLogDir, activeHost, ACTIVE_WAL_TS);
    fs.create(activeOldWal).close();
    String activeKey = activeHost.getAddress().toString();

    // olderTimestamps == previousTimestampMins (min per host from the PREVIOUS backup).
    Map<String, Long> olderTimestamps = new HashMap<>();
    olderTimestamps.put(deadKey, DEAD_FROZEN_BOUNDARY);
    olderTimestamps.put(activeKey, ACTIVE_PREV_BOUNDARY);

    // newestTimestamps == readRegionServerLastLogRollResult() after this cycle's roll. The dead
    // host is PRESENT but frozen (it did not participate); the active host advanced.
    Map<String, Long> newestTimestamps = new HashMap<>();
    newestTimestamps.put(deadKey, DEAD_FROZEN_BOUNDARY);
    newestTimestamps.put(activeKey, ACTIVE_FRESH_BOUNDARY);

    // ---- Preconditions: each is an independently checkable premise of the incident scenario ----
    // P1: the dead host's WAL really lives in oldWALs/ and is parsed as archived.
    assertTrue(AbstractFSWALProvider.isArchivedLogFile(deadOldWal),
      "fixture: dead host's WAL must be under oldWALs/");
    // P2: the crafted filename parses to the intended host and timestamp (matches production
    // logic).
    assertEquals(deadKey, BackupUtils.parseHostFromOldLog(deadOldWal),
      "fixture: parseHostFromOldLog must recover the dead host key used in the boundary maps");
    assertEquals(DEAD_LOST_WAL_TS, WAL.getTimestamp(deadOldWal.getName()),
      "fixture: WAL.getTimestamp must recover the WAL's creation timestamp");
    // P3: the dead host IS present in newestTimestamps (not null) with a STALE, frozen value.
    assertTrue(newestTimestamps.containsKey(deadKey),
      "incident condition: a decommissioned host is still present in rslogts (persistent table)");
    // P4: its un-backed-up WAL is newer than that frozen boundary -> exclusion clause is armed.
    assertTrue(DEAD_LOST_WAL_TS > newestTimestamps.get(deadKey),
      "incident condition: the dead host's archived WAL is newer than its frozen boundary");
    // P5: positive control is genuinely in range (prev < ts <= fresh), so it MUST be backed up.
    assertTrue(
      ACTIVE_WAL_TS > olderTimestamps.get(activeKey)
        && ACTIVE_WAL_TS <= newestTimestamps.get(activeKey),
      "fixture: active host's WAL must be strictly in the backup window");

    // ---- Behaviour under test ----
    List<String> logsToBackUp =
      invokeGetLogFilesForNewBackup(olderTimestamps, newestTimestamps, conf);
    Set<String> selectedNames =
      logsToBackUp.stream().map(s -> new Path(s).getName()).collect(Collectors.toSet());
    LOG.info("Logs selected for backup: {}", selectedNames);

    assertTrue(selectedNames.contains(activeOldWal.getName()),
      "positive control: active host's in-range archived WAL should be backed up. Selected="
        + selectedNames);
    assertTrue(selectedNames.contains(deadOldWal.getName()),
      "decommissioned host's archived WAL (" + deadOldWal.getName() + ") is final and MUST be "
        + "backed up to avoid data loss, but it was filtered out. Selected=" + selectedNames);
  }

  private static Path archivedWal(Path oldLogDir, ServerName server, long walTs) {
    // Archived WAL name: "<host>,<port>,<startcode>.<walTs>"; parseHostFromOldLog splits on ',' and
    // WAL.getTimestamp reads the trailing timestamp.
    return new Path(oldLogDir, server.toString() + BackupUtils.LOGNAME_SEPARATOR + walTs);
  }

  @SuppressWarnings("unchecked")
  private static List<String> invokeGetLogFilesForNewBackup(Map<String, Long> older,
    Map<String, Long> newest, Configuration conf) throws Exception {
    // getLogFilesForNewBackup only reads its parameters (no instance state / connection / system
    // table), so we allocate the manager without running its cluster-dependent constructor.
    IncrementalBackupManager mgr = allocateWithoutConstructor(IncrementalBackupManager.class);
    Method m = IncrementalBackupManager.class.getDeclaredMethod("getLogFilesForNewBackup",
      Map.class, Map.class, Configuration.class);
    m.setAccessible(true);
    return (List<String>) m.invoke(mgr, older, newest, conf);
  }

  @SuppressWarnings("unchecked")
  private static <T> T allocateWithoutConstructor(Class<T> clazz) throws Exception {
    Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
    Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
    theUnsafe.setAccessible(true);
    Object unsafe = theUnsafe.get(null);
    Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
    return (T) allocateInstance.invoke(unsafe, clazz);
  }
}
