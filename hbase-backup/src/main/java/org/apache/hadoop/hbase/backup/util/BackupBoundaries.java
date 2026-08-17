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
package org.apache.hadoop.hbase.backup.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks WAL cleanup boundaries separately for each backup root to ensure WALs are only deleted
 * when ALL backup roots no longer need them. A WAL file can only be deleted if it is older than the
 * boundary for every backup root, protecting WALs needed by any root even when other roots have
 * already backed up that host at a later timestamp.
 */
@InterfaceAudience.Private
public class BackupBoundaries {
  private static final Logger LOG = LoggerFactory.getLogger(BackupBoundaries.class);
  private static final BackupBoundaries EMPTY = new BackupBoundaries(Collections.emptyMap());

  private final Map<String, BoundaryInfo> rootBoundaries;

  private BackupBoundaries(Map<String, BoundaryInfo> rootBoundaries) {
    this.rootBoundaries = rootBoundaries;
  }

  public boolean isDeletable(Path walLogPath) {
    try {
      String hostname = BackupUtils.parseHostNameFromLogFile(walLogPath);

      if (hostname == null) {
        LOG.warn(
          "Cannot parse hostname from RegionServer WAL file: {}. Ignoring cleanup of this log",
          walLogPath);
        return false;
      }

      Address address = Address.fromString(hostname);
      long pathTs = WAL.getTimestamp(walLogPath.getName());

      for (Map.Entry<String, BoundaryInfo> entry : rootBoundaries.entrySet()) {
        if (!entry.getValue().isDeletable(address, pathTs)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Backup root {} preventing deletion of {} with ts {}", entry.getKey(),
              walLogPath, pathTs);
          }
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.warn("Error occurred while filtering file: {}. Ignoring cleanup of this log", walLogPath,
        e);
      return false;
    }
  }

  public void logAllBoundaries() {
    for (Map.Entry<String, BoundaryInfo> entry : rootBoundaries.entrySet()) {
      entry.getValue().logBoundaries(entry.getKey());
    }
  }

  public static BackupBoundariesBuilder builder(long tsCleanupBuffer) {
    return new BackupBoundariesBuilder(tsCleanupBuffer);
  }

  public static class BoundaryInfo {
    private final Map<Address, Long> boundaries;
    private final long defaultBoundary;

    private BoundaryInfo(Map<Address, Long> boundaries, long defaultBoundary) {
      this.boundaries = boundaries;
      this.defaultBoundary = defaultBoundary;
    }

    public boolean isDeletable(Address address, long pathTs) {
      Long boundary = boundaries.get(address);
      if (boundary == null) {
        return pathTs <= defaultBoundary;
      }
      return pathTs <= boundary;
    }

    public void logBoundaries(String rootDir) {
      LOG.debug("Backup root: {}, defaultBoundary: {}", rootDir, defaultBoundary);
      for (Map.Entry<Address, Long> entry : boundaries.entrySet()) {
        LOG.debug("Backup root: {}, Server: {}, WAL cleanup boundary: {}", rootDir,
          entry.getKey().getHostName(), entry.getValue());
      }
    }
  }

  public static class BackupBoundariesBuilder {
    private final Map<String, PerRootState> perRootStates = new HashMap<>();
    private final long tsCleanupBuffer;

    private BackupBoundariesBuilder(long tsCleanupBuffer) {
      this.tsCleanupBuffer = tsCleanupBuffer;
    }

    /**
     * Updates the boundaries based on the provided backup info. Boundaries are tracked per backup
     * root so that each root independently protects the WALs it still needs.
     * @param backupInfo the most recent completed backup info for a backup root, or if there is no
     *                   such completed backup, the currently running backup.
     */
    public void update(BackupInfo backupInfo) {
      PerRootState state =
        perRootStates.computeIfAbsent(backupInfo.getBackupRootDir(), k -> new PerRootState());

      switch (backupInfo.getState()) {
        case COMPLETE:
          for (TableName table : backupInfo.getTableSetTimestampMap().keySet()) {
            for (Map.Entry<String, Long> entry : backupInfo.getTableSetTimestampMap().get(table)
              .entrySet()) {
              Address regionServerAddress = Address.fromString(entry.getKey());
              Long logRollTs = entry.getValue();

              Long storedTs = state.boundaries.get(regionServerAddress);
              if (storedTs == null || logRollTs < storedTs) {
                state.boundaries.put(regionServerAddress, logRollTs);
                if (logRollTs < state.oldestRollTs) {
                  state.oldestRollTs = logRollTs;
                }
              }
            }
          }
          break;
        case RUNNING:
          state.oldestStartTs = Math.min(state.oldestStartTs, backupInfo.getStartTs());
          break;
        default:
          throw new IllegalStateException("Unexpected backupInfo state: " + backupInfo.getState());
      }
    }

    public BackupBoundaries build() {
      if (perRootStates.isEmpty()) {
        return EMPTY;
      }

      Map<String, BoundaryInfo> rootBoundaries = new HashMap<>();
      for (Map.Entry<String, PerRootState> entry : perRootStates.entrySet()) {
        PerRootState state = entry.getValue();
        long defaultBoundary;
        if (state.boundaries.isEmpty()) {
          defaultBoundary = state.oldestStartTs - tsCleanupBuffer;
        } else {
          defaultBoundary = Math.min(state.oldestRollTs, state.oldestStartTs) - tsCleanupBuffer;
        }
        rootBoundaries.put(entry.getKey(), new BoundaryInfo(state.boundaries, defaultBoundary));
      }
      return new BackupBoundaries(rootBoundaries);
    }

    private static class PerRootState {
      final Map<Address, Long> boundaries = new HashMap<>();
      long oldestStartTs = Long.MAX_VALUE;
      long oldestRollTs = Long.MAX_VALUE;
    }
  }
}
