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
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * Mob file compaction chore in a generational non-batch mode test. 1. Uses default (non-batch) mode
 * for regular MOB compaction, sets generational mode ON 2. Disables periodic MOB compactions, sets
 * minimum age to archive to 10 sec 3. Creates MOB table with 20 regions 4. Loads MOB data
 * (randomized keys, 1000 rows), flushes data. 5. Repeats 4. two more times 6. Verifies that we have
 * 20 *3 = 60 mob files (equals to number of regions x 3) 7. Runs major MOB compaction. 8. Verifies
 * that number of MOB files in a mob directory is 20 x4 = 80 9. Waits for a period of time larger
 * than minimum age to archive 10. Runs Mob cleaner chore 11 Verifies that number of MOB files in a
 * mob directory is 20. 12 Runs scanner and checks all 3 * 1000 rows.
 */
@Category(LargeTests.class)
public class TestMobCompactionOptMode extends TestMobCompactionWithDefaults {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobCompactionOptMode.class);

  public TestMobCompactionOptMode(Boolean useFileBasedSFT) {
    super(useFileBasedSFT);
  }

  protected void additonalConfigSetup() {
    conf.set(MobConstants.MOB_COMPACTION_TYPE_KEY, MobConstants.OPTIMIZED_MOB_COMPACTION_TYPE);
    conf.setLong(MobConstants.MOB_COMPACTION_MAX_FILE_SIZE_KEY, 1000000);
  }

  @Override
  protected String description() {
    return "generational (non-batch) mode";
  }

}
