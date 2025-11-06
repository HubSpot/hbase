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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A cache implementation for region locations from meta.
 */
@InterfaceAudience.Private
public class MetaCache {

  private static final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

  private final Supplier<Boolean> extraMetaCacheClearingEnabled;  // this is a live config
  private final Supplier<Long> extraMetaCacheDebounceTimeoutMillis;  // this is a live config
  private final ScheduledExecutorService repopulateExecutorService;
  private final Cache<TableName, ScheduledFuture<?>> tableRefreshesRan;
  private static final String REPOPULATOR_THREAD_PREFIX = "meta-cache-repopulator-";


  /**
   * Map of table to table {@link HRegionLocation}s. <br>
   * Despite being Concurrent, writes to the map should be synchronized because we have cases where
   * we need to make multiple updates atomically.
   */
  private final ConcurrentMap<TableName,
    ConcurrentNavigableMap<byte[], RegionLocations>> cachedRegionLocations =
      new CopyOnWriteArrayMap<>();

  // The presence of a server in the map implies it's likely that there is an
  // entry in cachedRegionLocations that map to this server; but the absence
  // of a server in this map guarantees that there is no entry in cache that
  // maps to the absent server.
  // The access to this attribute must be protected by a lock on cachedRegionLocations
  private final Set<ServerName> cachedServers = new CopyOnWriteArraySet<>();

  private final MetricsConnection metrics;

  private final ClusterConnection connection;


  public MetaCache(MetricsConnection metrics, ClusterConnection connection) {
    this.metrics = metrics;

    // HubSpot modification: at most every hubspot.meta.cache.debounce.timeout.ms, entirely clear and repopulate the
    // meta cache for a table which has recently experienced meta clearing errors. We do this by recording the table name
    // when the cache is cleared for a region, and then after that timeout, clearing the cache for the whole table
    // in a background thread.
    this.connection = connection;
    Configuration conf = connection.getConfiguration();

    extraMetaCacheClearingEnabled = () -> conf.getBoolean("hubspot.meta.cache.extra.clearing", false);
    extraMetaCacheDebounceTimeoutMillis = () -> conf.getLong("hubspot.meta.cache.debounce.timeout.ms", TimeUnit.MINUTES.toMillis(10));

    repopulateExecutorService = new ScheduledThreadPoolExecutor(1,
      new ThreadFactoryBuilder().setNameFormat(REPOPULATOR_THREAD_PREFIX + "%s").setDaemon(true).build(),
      new DiscardPolicy());

    tableRefreshesRan = CacheBuilder
      .newBuilder()
      .maximumSize(conf.getLong("hubspot.meta.cache.debounce.cache.size", 1000))
      .build();

  }

  /**
   * Search the cache for a location that fits our table and row key. Return null if no suitable
   * region is located.
   * @return Null or region location found in cache.
   */
  public RegionLocations getCachedLocation(final TableName tableName, final byte[] row) {
    ConcurrentNavigableMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    Entry<byte[], RegionLocations> e = tableLocations.floorEntry(row);
    if (e == null) {
      if (metrics != null) metrics.incrMetaCacheMiss();
      return null;
    }
    RegionLocations possibleRegion = e.getValue();

    // make sure that the end key is greater than the row we're looking
    // for, otherwise the row actually belongs in the next region, not
    // this one. the exception case is when the endkey is
    // HConstants.EMPTY_END_ROW, signifying that the region we're
    // checking is actually the last region in the table.
    byte[] endKey = possibleRegion.getRegionLocation().getRegion().getEndKey();
    // Here we do direct Bytes.compareTo and not doing CellComparator/MetaCellComparator path.
    // MetaCellComparator is for comparing against data in META table which need special handling.
    // Not doing that is ok for this case because
    // 1. We are getting the Region location for the given row in non META tables only. The compare
    // checks the given row is within the end key of the found region. So META regions are not
    // coming in here.
    // 2. Even if META region comes in, its end key will be empty byte[] and so Bytes.equals(endKey,
    // HConstants.EMPTY_END_ROW) check itself will pass.
    if (
      Bytes.equals(endKey, HConstants.EMPTY_END_ROW)
        || Bytes.compareTo(endKey, 0, endKey.length, row, 0, row.length) > 0
    ) {
      if (metrics != null) metrics.incrMetaCacheHit();
      return possibleRegion;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Requested row {} comes after region end key of {} for cached location {}",
        Bytes.toStringBinary(row), Bytes.toStringBinary(endKey), possibleRegion);
    }
    // Passed all the way through, so we got nothing - complete cache miss
    if (metrics != null) metrics.incrMetaCacheMiss();
    return null;
  }

  /**
   * Put a newly discovered HRegionLocation into the cache. Synchronize here because we may need to
   * make multiple modifications in cleanProblematicOverlappedRegions, and we want them to be
   * atomic.
   * @param tableName The table name.
   * @param source    the source of the new location
   * @param location  the new location
   */
  public synchronized void cacheLocation(final TableName tableName, final ServerName source,
    final HRegionLocation location) {
    assert source != null;
    byte[] startKey = location.getRegion().getStartKey();
    ConcurrentNavigableMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);
    RegionLocations locations = new RegionLocations(new HRegionLocation[] { location });
    RegionLocations oldLocations = tableLocations.putIfAbsent(startKey, locations);
    boolean isNewCacheEntry = (oldLocations == null);
    if (isNewCacheEntry) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cached location: " + location);
      }
      addToCachedServers(locations);
      MetaCacheUtil.cleanProblematicOverlappedRegions(locations, tableLocations);
      return;
    }

    // If the server in cache sends us a redirect, assume it's always valid.
    HRegionLocation oldLocation =
      oldLocations.getRegionLocation(location.getRegion().getReplicaId());
    boolean force = oldLocation != null && oldLocation.getServerName() != null
      && oldLocation.getServerName().equals(source);

    // For redirect if the number is equal to previous
    // record, the most common case is that first the region was closed with seqNum, and then
    // opened with the same seqNum; hence we will ignore the redirect.
    // There are so many corner cases with various combinations of opens and closes that
    // an additional counter on top of seqNum would be necessary to handle them all.
    RegionLocations updatedLocations = oldLocations.updateLocation(location, false, force);
    if (oldLocations != updatedLocations) {
      tableLocations.put(startKey, updatedLocations);
      MetaCacheUtil.cleanProblematicOverlappedRegions(updatedLocations, tableLocations);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Changed cached location to: " + location);
      }
      addToCachedServers(updatedLocations);
    }
  }

  /**
   * Put a newly discovered HRegionLocation into the cache. Synchronize here because we may need to
   * make multiple modifications in cleanProblematicOverlappedRegions, and we want them to be
   * atomic.
   * @param tableName The table name.
   * @param locations the new locations
   */
  public synchronized void cacheLocation(final TableName tableName,
    final RegionLocations locations) {
    byte[] startKey = locations.getRegionLocation().getRegion().getStartKey();
    ConcurrentNavigableMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);
    RegionLocations oldLocation = tableLocations.putIfAbsent(startKey, locations);
    boolean isNewCacheEntry = (oldLocation == null);
    if (isNewCacheEntry) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cached location: " + locations);
      }
      addToCachedServers(locations);
      MetaCacheUtil.cleanProblematicOverlappedRegions(locations, tableLocations);
      return;
    }

    // merge old and new locations and add it to the cache
    // Meta record might be stale - some (probably the same) server has closed the region
    // with later seqNum and told us about the new location.
    RegionLocations mergedLocation = oldLocation.mergeLocations(locations);
    tableLocations.put(startKey, mergedLocation);
    MetaCacheUtil.cleanProblematicOverlappedRegions(mergedLocation, tableLocations);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Merged cached locations: " + mergedLocation);
    }
    addToCachedServers(locations);
  }

  private void addToCachedServers(RegionLocations locations) {
    for (HRegionLocation loc : locations.getRegionLocations()) {
      if (loc != null) {
        cachedServers.add(loc.getServerName());
      }
    }
  }

  /**
   * Returns Map of cached locations for passed <code>tableName</code>.<br>
   * Despite being Concurrent, writes to the map should be synchronized because we have cases where
   * we need to make multiple updates atomically.
   */
  private ConcurrentNavigableMap<byte[], RegionLocations>
    getTableLocations(final TableName tableName) {
    // find the map of cached locations for this table
    return computeIfAbsent(cachedRegionLocations, tableName,
      () -> new CopyOnWriteArrayMap<>(Bytes.BYTES_COMPARATOR));
  }

  /**
   * Check the region cache to see whether a region is cached yet or not.
   * @param tableName tableName
   * @param row       row
   * @return Region cached or not.
   */
  public boolean isRegionCached(TableName tableName, final byte[] row) {
    RegionLocations location = getCachedLocation(tableName, row);
    return location != null;
  }

  /**
   * Return the number of cached region for a table. It will only be called from a unit test.
   */
  public int getNumberOfCachedRegionLocations(final TableName tableName) {
    Map<byte[], RegionLocations> tableLocs = this.cachedRegionLocations.get(tableName);
    if (tableLocs == null) {
      return 0;
    }
    int numRegions = 0;
    for (RegionLocations tableLoc : tableLocs.values()) {
      numRegions += tableLoc.numNonNullElements();
    }
    return numRegions;
  }

  /**
   * Delete all cached entries. <br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   */
  public synchronized void clearCache() {
    this.cachedRegionLocations.clear();
    this.cachedServers.clear();
  }

  /**
   * Delete all cached entries of a server. <br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   */
  public synchronized void clearCache(final ServerName serverName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received request to clear cache for server {}", serverName);
    }
    // Prior to synchronizing this method, we used to do another check below while synchronizing
    // on cachedServers. This is no longer necessary since we moved synchronization up.
    // Prior reason:
    // We block here, because if there is an error on a server, it's likely that multiple
    // threads will get the error simultaneously. If there are hundreds of thousand of
    // region location to check, it's better to do this only once. A better pattern would
    // be to check if the server is dead when we get the region location.
    if (!this.cachedServers.contains(serverName)) {
      return;
    }

    boolean deletedSomething = false;
    for (ConcurrentMap<byte[], RegionLocations> tableLocations : cachedRegionLocations.values()) {
      for (Entry<byte[], RegionLocations> e : tableLocations.entrySet()) {
        RegionLocations regionLocations = e.getValue();
        if (regionLocations != null) {
          RegionLocations updatedLocations = regionLocations.removeByServer(serverName);
          if (updatedLocations != regionLocations) {
            deletedSomething = true;
            if (updatedLocations.isEmpty()) {
              tableLocations.remove(e.getKey());
            } else {
              tableLocations.put(e.getKey(), updatedLocations);
            }
          }
        }
      }
    }
    this.cachedServers.remove(serverName);
    if (deletedSomething) {
      if (metrics != null) {
        metrics.incrMetaCacheNumClearServer();
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removed all cached region locations that map to " + serverName);
      }
    }
  }

  /**
   * Delete a cached location, no matter what it is. Called when we were told to not use cache.<br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   * @param tableName tableName
   */
  public synchronized void clearCache(final TableName tableName, final byte[] row) {
    // HubSpot modification
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received request to clear cache for table/row {}/{}", tableName, Bytes.toStringBinary(row));
    }
    delayedCacheRefreshForTable(tableName);

    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      byte[] startKey = regionLocations.getRegionLocation().getRegion().getStartKey();
      tableLocations.remove(startKey);
      if (metrics != null) {
        metrics.incrMetaCacheNumClearRegion();
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removed " + regionLocations + " from cache");
      }
    }
  }

  /**
   * Delete all cached entries of a table.<br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   */
  public synchronized void clearCache(final TableName tableName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removing all cached region locations for table " + tableName);
    }
    this.cachedRegionLocations.remove(tableName);
  }

  /**
   * Delete a cached location with specific replicaId.<br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   * @param tableName tableName
   * @param row       row key
   * @param replicaId region replica id
   */
  public synchronized void clearCache(final TableName tableName, final byte[] row, int replicaId) {
    // HubSpot modification
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received request to clear cache for table/row/replica {}/{}/{}", tableName, Bytes.toStringBinary(row), replicaId);
    }
    delayedCacheRefreshForTable(tableName);

    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      HRegionLocation toBeRemoved = regionLocations.getRegionLocation(replicaId);
      if (toBeRemoved != null) {
        RegionLocations updatedLocations = regionLocations.remove(replicaId);
        byte[] startKey = regionLocations.getRegionLocation().getRegion().getStartKey();
        if (updatedLocations.isEmpty()) {
          tableLocations.remove(startKey);
        } else {
          tableLocations.put(startKey, updatedLocations);
        }

        if (metrics != null) {
          metrics.incrMetaCacheNumClearRegion();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removed " + toBeRemoved + " from cache");
        }
      }
    }
  }

  /**
   * Delete a cached location for a table, row and server. <br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   */
  public synchronized void clearCache(final TableName tableName, final byte[] row,
    ServerName serverName) {
    // HubSpot modification
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received request to clear cache for table/row/server {}/{}/{}", tableName, Bytes.toStringBinary(row), serverName);
    }
    delayedCacheRefreshForTable(tableName);

    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      RegionLocations updatedLocations = regionLocations.removeByServer(serverName);
      if (updatedLocations != regionLocations) {
        byte[] startKey = regionLocations.getRegionLocation().getRegion().getStartKey();
        if (updatedLocations.isEmpty()) {
          tableLocations.remove(startKey);
        } else {
          tableLocations.put(startKey, updatedLocations);
        }
        if (metrics != null) {
          metrics.incrMetaCacheNumClearRegion();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removed locations of table: " + tableName + " ,row: " + Bytes.toString(row)
            + " mapping to server: " + serverName + " from cache");
        }
      }
    }
  }

  /**
   * Deletes the cached location of the region if necessary, based on some error from source.<br>
   * Synchronized because of calls in cacheLocation which need to be executed atomically
   * @param hri The region in question.
   */
  public synchronized void clearCache(RegionInfo hri) {
    // HubSpot modification
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received request to clear cache for region {}", hri.getShortNameToLog());
    }
    delayedCacheRefreshForTable(hri.getTable());

    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(hri.getTable());
    RegionLocations regionLocations = tableLocations.get(hri.getStartKey());
    if (regionLocations != null) {
      HRegionLocation oldLocation = regionLocations.getRegionLocation(hri.getReplicaId());
      if (oldLocation == null) return;
      RegionLocations updatedLocations = regionLocations.remove(oldLocation);
      if (updatedLocations != regionLocations) {
        if (updatedLocations.isEmpty()) {
          tableLocations.remove(hri.getStartKey());
        } else {
          tableLocations.put(hri.getStartKey(), updatedLocations);
        }
        if (metrics != null) {
          metrics.incrMetaCacheNumClearRegion();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removed " + oldLocation + " from cache");
        }
      }
    }
  }

  /**
   * HubSpot addition
   * Clear and proactively repopulate the cache for each region in a given table. This is expensive so we should only do it
   * occasionally. Don't run this in a critical path because it's doing a bunch of network calls.
   */
  private void repopulateCacheForTable(TableName tableName) {
    // check again in case it changed during debounce time
    if (!extraMetaCacheClearingEnabled.get()) {
      LOG.debug("Not doing meta cache refresh because feature is disabled");
      return;
    }

    try {
      LOG.debug("Running metaScan for table " + tableName.getNameAsString());
      MetaTableAccessor.scanMetaForTableRegions(this.connection, new CacheRegionLocationMetaVisitor(tableName), tableName);
    } catch (Exception e) {
      LOG.warn("Error while repopulating meta cache for table " + tableName);
    }
  }

  private void delayedCacheRefreshForTable(TableName tableName) {
    if (!extraMetaCacheClearingEnabled.get()) {
      LOG.debug("Not scheduling meta cache refresh because feature is disabled");
      return;
    }
    if (Thread.currentThread().getName().startsWith(REPOPULATOR_THREAD_PREFIX)) {
      LOG.debug("Not scheduling meta cache refresh because we were called from within a refresh itself");
      return;
    }
    synchronized (tableRefreshesRan) {
      ScheduledFuture<?> lastScheduledRefresh = tableRefreshesRan.getIfPresent(tableName);
      if (lastScheduledRefresh != null && lastScheduledRefresh.isDone()) {
        tableRefreshesRan.invalidate(tableName);
      } else if (lastScheduledRefresh != null && !lastScheduledRefresh.isDone()) {
        LOG.debug("Not scheduling meta cache refresh because one has already been scheduled or is in progress for table " + tableName.getNameAsString());
        return;
      }

      LOG.debug("Scheduling cache refresh for table " + tableName.getNameAsString());
      ScheduledFuture<?> future = repopulateExecutorService.schedule(() -> repopulateCacheForTable(tableName), extraMetaCacheDebounceTimeoutMillis.get(), TimeUnit.MILLISECONDS);
      tableRefreshesRan.put(tableName, future);
    }

  }

  /** HubSpot addition */
  private class CacheRegionLocationMetaVisitor implements MetaTableAccessor.Visitor {

    private final TableName tableName;

    private String printRegion(HRegionLocation regionLocation) {
      return regionLocation.getRegion().toString();
    }

    public CacheRegionLocationMetaVisitor(TableName tableName) {
      this.tableName = tableName;
    }

    @Override public boolean visit(Result rowResult) throws IOException {
      RegionLocations locations = MetaTableAccessor.getRegionLocations(rowResult);
      if (locations == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Locations is null");
        }
        return true;
      }

      // The assumption in the MetaCache is that location.getServerName() is never null. Otherwise,
      // NPEs can arise in a number of places. If a null servername is found, just skip caching it.
      // It will be fetched through normal means later.
      if (anyServerNameNull(locations)) {
        return true;
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Regions: " + Arrays.stream(locations.getRegionLocations()).map(this::printRegion).collect(
          Collectors.joining("\n")));
      }
      cacheLocation(tableName, locations);
      return true;
    }

    private boolean anyServerNameNull(RegionLocations locations) {
      for (HRegionLocation location : locations.getRegionLocations()) {
        if (location.getServerName() == null) {
          LOG.trace("ServerName for location " + location + " is null");
          return true;
        }
      }
      return false;
    }
  }

}
