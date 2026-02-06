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
package org.apache.hadoop.hbase.io.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Arrays;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests LRUDictionary
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestLRUDictionary {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLRUDictionary.class);

  LRUDictionary testee;

  @Before
  public void setUp() throws Exception {
    testee = new LRUDictionary();
    testee.init(Short.MAX_VALUE);
  }

  @Test
  public void TestContainsNothing() {
    assertTrue(isDictionaryEmpty(testee));
  }

  /**
   * Assert can't add empty array.
   */
  @Test
  public void testPassingEmptyArrayToFindEntry() {
    assertEquals(Dictionary.NOT_IN_DICTIONARY, testee.findEntry(HConstants.EMPTY_BYTE_ARRAY, 0, 0));
    assertEquals(Dictionary.NOT_IN_DICTIONARY, testee.addEntry(HConstants.EMPTY_BYTE_ARRAY, 0, 0));
  }

  @Test
  public void testPassingSameArrayToAddEntry() {
    // Add random predefined byte array, in this case a random byte array from
    // HConstants. Assert that when we add, we get new index. Thats how it
    // works.
    int len = HConstants.CATALOG_FAMILY.length;
    int index = testee.addEntry(HConstants.CATALOG_FAMILY, 0, len);
    assertFalse(index == testee.addEntry(HConstants.CATALOG_FAMILY, 0, len));
    assertFalse(index == testee.addEntry(HConstants.CATALOG_FAMILY, 0, len));
  }

  @Test
  public void testBasic() {
    byte[] testBytes = new byte[10];
    Bytes.random(testBytes);

    // Verify that our randomly generated array doesn't exist in the dictionary
    assertEquals(-1, testee.findEntry(testBytes, 0, testBytes.length));

    // now since we looked up an entry, we should have added it to the
    // dictionary, so it isn't empty

    assertFalse(isDictionaryEmpty(testee));

    // Check if we can find it using findEntry
    short t = testee.findEntry(testBytes, 0, testBytes.length);

    // Making sure we do find what we're looking for
    assertTrue(t != -1);

    byte[] testBytesCopy = new byte[20];

    Bytes.putBytes(testBytesCopy, 10, testBytes, 0, testBytes.length);

    // copy byte arrays, make sure that we check that equal byte arrays are
    // equal without just checking the reference
    assertEquals(testee.findEntry(testBytesCopy, 10, testBytes.length), t);

    // make sure the entry retrieved is the same as the one put in
    assertTrue(Arrays.equals(testBytes, testee.getEntry(t)));

    testee.clear();

    // making sure clear clears the dictionary
    assertTrue(isDictionaryEmpty(testee));
  }

  @Test
  public void TestLRUPolicy() {
    // start by filling the dictionary up with byte arrays
    for (int i = 0; i < Short.MAX_VALUE; i++) {
      testee.findEntry(BigInteger.valueOf(i).toByteArray(), 0,
        BigInteger.valueOf(i).toByteArray().length);
    }

    // check we have the first element added
    assertTrue(
      testee.findEntry(BigInteger.ZERO.toByteArray(), 0, BigInteger.ZERO.toByteArray().length)
          != -1);

    // check for an element we know isn't there
    assertTrue(testee.findEntry(BigInteger.valueOf(Integer.MAX_VALUE).toByteArray(), 0,
      BigInteger.valueOf(Integer.MAX_VALUE).toByteArray().length) == -1);

    // since we just checked for this element, it should be there now.
    assertTrue(testee.findEntry(BigInteger.valueOf(Integer.MAX_VALUE).toByteArray(), 0,
      BigInteger.valueOf(Integer.MAX_VALUE).toByteArray().length) != -1);

    // test eviction, that the least recently added or looked at element is
    // evicted. We looked at ZERO so it should be in the dictionary still.
    assertTrue(
      testee.findEntry(BigInteger.ZERO.toByteArray(), 0, BigInteger.ZERO.toByteArray().length)
          != -1);
    // Now go from beyond 1 to the end.
    for (int i = 1; i < Short.MAX_VALUE; i++) {
      assertTrue(testee.findEntry(BigInteger.valueOf(i).toByteArray(), 0,
        BigInteger.valueOf(i).toByteArray().length) == -1);
    }

    // check we can find all of these.
    for (int i = 0; i < Short.MAX_VALUE; i++) {
      assertTrue(testee.findEntry(BigInteger.valueOf(i).toByteArray(), 0,
        BigInteger.valueOf(i).toByteArray().length) != -1);
    }
  }

  @Test
  public void testSavepointRollbackUndoesAdd() {
    byte[] existingEntry = Bytes.toBytes("before-savepoint");
    testee.addEntry(existingEntry, 0, existingEntry.length);
    short existingIdx = testee.findEntry(existingEntry, 0, existingEntry.length);
    assertTrue(existingIdx != -1);

    testee.savepoint();

    byte[] newEntry = Bytes.toBytes("after-savepoint");
    testee.addEntry(newEntry, 0, newEntry.length);
    short newIdx = testee.findEntry(newEntry, 0, newEntry.length);
    assertTrue(newIdx != -1);

    testee.rollback();

    assertTrue(testee.findEntry(existingEntry, 0, existingEntry.length) != -1);
    assertTrue(Arrays.equals(existingEntry, testee.getEntry(existingIdx)));

    assertEquals(-1, testee.findEntry(newEntry, 0, newEntry.length));
  }

  @Test
  public void testSavepointRollbackUndoesGetReorder() {
    byte[] entry1 = Bytes.toBytes("entry1");
    byte[] entry2 = Bytes.toBytes("entry2");
    byte[] entry3 = Bytes.toBytes("entry3");

    testee.addEntry(entry1, 0, entry1.length);
    testee.addEntry(entry2, 0, entry2.length);
    testee.addEntry(entry3, 0, entry3.length);

    short idx1 = testee.findEntry(entry1, 0, entry1.length);
    short idx2 = testee.findEntry(entry2, 0, entry2.length);
    short idx3 = testee.findEntry(entry3, 0, entry3.length);

    testee.savepoint();

    testee.getEntry(idx1);

    testee.rollback();

    assertTrue(Arrays.equals(entry1, testee.getEntry(idx1)));
    assertTrue(Arrays.equals(entry2, testee.getEntry(idx2)));
    assertTrue(Arrays.equals(entry3, testee.getEntry(idx3)));
  }

  @Test
  public void testSavepointRollbackWithEviction() {
    LRUDictionary smallDict = new LRUDictionary();
    smallDict.init(3);

    byte[] a = Bytes.toBytes("aaa");
    byte[] b = Bytes.toBytes("bbb");
    byte[] c = Bytes.toBytes("ccc");

    smallDict.addEntry(a, 0, a.length);
    smallDict.addEntry(b, 0, b.length);
    smallDict.addEntry(c, 0, c.length);

    short idxA = smallDict.findEntry(a, 0, a.length);
    short idxB = smallDict.findEntry(b, 0, b.length);
    short idxC = smallDict.findEntry(c, 0, c.length);
    assertTrue(idxA != -1);
    assertTrue(idxB != -1);
    assertTrue(idxC != -1);

    smallDict.savepoint();

    byte[] d = Bytes.toBytes("ddd");
    smallDict.addEntry(d, 0, d.length);

    smallDict.rollback();

    assertTrue(Arrays.equals(a, smallDict.getEntry(idxA)));
    assertTrue(Arrays.equals(b, smallDict.getEntry(idxB)));
    assertTrue(Arrays.equals(c, smallDict.getEntry(idxC)));
    assertEquals(-1, smallDict.findEntry(d, 0, d.length));
  }

  @Test
  public void testSavepointRelease() {
    byte[] entry = Bytes.toBytes("persist-me");
    testee.savepoint();
    testee.addEntry(entry, 0, entry.length);
    testee.releaseSavepoint();

    short idx = testee.findEntry(entry, 0, entry.length);
    assertTrue(idx != -1);
    assertTrue(Arrays.equals(entry, testee.getEntry(idx)));
  }

  @Test
  public void testSavepointRollbackEmpty() {
    byte[] entry = Bytes.toBytes("existing");
    testee.addEntry(entry, 0, entry.length);

    testee.savepoint();
    testee.rollback();

    short idx = testee.findEntry(entry, 0, entry.length);
    assertTrue(idx != -1);
    assertTrue(Arrays.equals(entry, testee.getEntry(idx)));
  }

  @Test
  public void testSavepointRollbackInterleavedOps() {
    LRUDictionary smallDict = new LRUDictionary();
    smallDict.init(4);

    byte[] a = Bytes.toBytes("aaa");
    byte[] b = Bytes.toBytes("bbb");
    smallDict.addEntry(a, 0, a.length);
    smallDict.addEntry(b, 0, b.length);

    short idxA = smallDict.findEntry(a, 0, a.length);
    short idxB = smallDict.findEntry(b, 0, b.length);

    smallDict.savepoint();

    byte[] c = Bytes.toBytes("ccc");
    smallDict.addEntry(c, 0, c.length);

    smallDict.getEntry(idxA);

    byte[] d = Bytes.toBytes("ddd");
    smallDict.addEntry(d, 0, d.length);

    smallDict.getEntry(idxB);

    smallDict.rollback();

    assertTrue(Arrays.equals(a, smallDict.getEntry(idxA)));
    assertTrue(Arrays.equals(b, smallDict.getEntry(idxB)));
    assertEquals(-1, smallDict.findEntry(c, 0, c.length));
    assertEquals(-1, smallDict.findEntry(d, 0, d.length));
  }

  static private boolean isDictionaryEmpty(LRUDictionary dict) {
    try {
      dict.getEntry((short) 0);
      return false;
    } catch (IndexOutOfBoundsException ioobe) {
      return true;
    }
  }
}
