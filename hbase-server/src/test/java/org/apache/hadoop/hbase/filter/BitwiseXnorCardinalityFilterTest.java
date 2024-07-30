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
package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.assertEquals;

@Category({ FilterTests.class, MediumTests.class })
public class BitwiseXnorCardinalityFilterTest extends FilterTestingCluster {

  @Test
  public void testSerde() throws IOException, DeserializationException {
    BitwiseXnorCardinalityFilter filter = new BitwiseXnorCardinalityFilter(2, new boolean[] { false, false, true});
    byte[] bytes = filter.toByteArray();
    BitwiseXnorCardinalityFilter filter2 =  BitwiseXnorCardinalityFilter.parseFrom(bytes);
    assertEquals(filter, filter2);
  }
}
