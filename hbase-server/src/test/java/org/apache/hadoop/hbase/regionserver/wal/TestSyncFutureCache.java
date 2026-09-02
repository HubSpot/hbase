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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestSyncFutureCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncFutureCache.class);

  @Test
  public void testFallsBackToNewSyncFutureWhenCacheThrows() throws Exception {
    SyncFutureCache cache = new SyncFutureCache(HBaseConfiguration.create());

    Cache<?, ?> throwing = Mockito.mock(Cache.class);
    Mockito.when(throwing.asMap()).thenThrow(new NullPointerException("boom"));

    Field field = SyncFutureCache.class.getDeclaredField("syncFutureCache");
    field.setAccessible(true);
    field.set(cache, throwing);

    assertNotNull(cache.getIfPresentOrNew());
  }
}
