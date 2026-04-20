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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestRegionReplicaSplitBatches {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicaSplitBatches.class);

  private static final TableName TABLE_NAME = TableName.valueOf("TestTable");
  private static final byte[] REGION = Bytes.toBytes("region");
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private Entry createEntry(int valueSize) {
    byte[] value = new byte[valueSize];
    WALEdit edit = new WALEdit();
    edit.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes("row"))
      .setFamily(FAMILY).setQualifier(Bytes.toBytes("q")).setValue(value).setType(Cell.Type.Put)
      .setTimestamp(1).build());
    return new Entry(new WALKeyImpl(REGION, TABLE_NAME, 1), edit);
  }

  private long estimateEntrySize(Entry entry) {
    return entry.getKey().estimatedSerializedSizeOf() + entry.getEdit().estimatedSerializedSizeOf();
  }

  @Test
  public void itShouldReturnSingleBatchWhenUnderLimit() {
    List<Entry> entries = new ArrayList<>();
    entries.add(createEntry(100));
    entries.add(createEntry(100));

    List<List<Entry>> batches =
      RegionReplicaReplicationEndpoint.splitBatches(entries, Integer.MAX_VALUE);

    assertEquals(1, batches.size());
    assertEquals(2, batches.get(0).size());
  }

  @Test
  public void itShouldSplitWhenExceedingLimit() {
    Entry entry = createEntry(100);
    long entrySize = estimateEntrySize(entry);
    // Set limit so that 2 entries fit but 3 do not
    int limit = (int) (entrySize * 2 + 1);

    List<Entry> entries = new ArrayList<>();
    entries.add(createEntry(100));
    entries.add(createEntry(100));
    entries.add(createEntry(100));

    List<List<Entry>> batches = RegionReplicaReplicationEndpoint.splitBatches(entries, limit);

    assertEquals(2, batches.size());
    assertEquals(2, batches.get(0).size());
    assertEquals(1, batches.get(1).size());
  }

  @Test
  public void itShouldHandleSingleEntryExceedingLimit() {
    List<Entry> entries = new ArrayList<>();
    entries.add(createEntry(10000));

    List<List<Entry>> batches = RegionReplicaReplicationEndpoint.splitBatches(entries, 1);

    assertEquals(1, batches.size());
    assertEquals(1, batches.get(0).size());
  }

  @Test
  public void itShouldHandleEmptyList() {
    List<List<Entry>> batches =
      RegionReplicaReplicationEndpoint.splitBatches(new ArrayList<>(), 1000);

    assertTrue(batches.isEmpty());
  }

  @Test
  public void itShouldPreserveEntryOrder() {
    Entry e1 = createEntry(100);
    Entry e2 = createEntry(100);
    Entry e3 = createEntry(100);
    long entrySize = estimateEntrySize(e1);
    int limit = (int) (entrySize + 1);

    List<Entry> entries = new ArrayList<>();
    entries.add(e1);
    entries.add(e2);
    entries.add(e3);

    List<List<Entry>> batches = RegionReplicaReplicationEndpoint.splitBatches(entries, limit);

    assertEquals(3, batches.size());
    assertSame(e1, batches.get(0).get(0));
    assertSame(e2, batches.get(1).get(0));
    assertSame(e3, batches.get(2).get(0));
  }
}
