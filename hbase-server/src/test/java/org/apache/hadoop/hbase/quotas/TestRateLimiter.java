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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.viewfs.TestNNStartupWhenViewFSOverloadSchemeEnabled;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verify the behaviour of the Rate Limiter.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestRateLimiter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRateLimiter.class);

  @Test
  public void testWaitIntervalTimeUnitSeconds() {
    testWaitInterval(TimeUnit.SECONDS, 10, 100);
  }

  @Test
  public void testWaitIntervalTimeUnitMinutes() {
    testWaitInterval(TimeUnit.MINUTES, 10, 6000);
  }

  @Test
  public void testWaitIntervalTimeUnitHours() {
    testWaitInterval(TimeUnit.HOURS, 10, 360000);
  }

  @Test
  public void testWaitIntervalTimeUnitDays() {
    testWaitInterval(TimeUnit.DAYS, 10, 8640000);
  }

  private void testWaitInterval(final TimeUnit timeUnit, final long limit,
    final long expectedWaitInterval) {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(limit, timeUnit);

    long nowTs = 0;
    // consume all the available resources, one request at the time.
    // the wait interval should be 0
    for (int i = 0; i < (limit - 1); ++i) {
      assertEquals(0, limiter.getWaitIntervalMs());
      limiter.consume();
      long waitInterval = limiter.waitInterval();
      assertEquals(0, waitInterval);
    }

    for (int i = 0; i < (limit * 4); ++i) {
      // There is one resource available, so we should be able to
      // consume it without waiting.
      limiter.setNextRefillTime(limiter.getNextRefillTime() - nowTs);
      assertEquals(0, limiter.getWaitIntervalMs());
      assertEquals(0, limiter.waitInterval());
      limiter.consume();
      // No more resources are available, we should wait for at least an interval.
      long waitInterval = limiter.waitInterval();
      assertEquals(expectedWaitInterval, waitInterval);

      // set the nowTs to be the exact time when resources should be available again.
      nowTs = waitInterval;

      // artificially go into the past to prove that when too early we should fail.
      long temp = nowTs + 500;
      limiter.setNextRefillTime(limiter.getNextRefillTime() + temp);
      assertNotEquals(0, limiter.getWaitIntervalMs());
      // Roll back the nextRefillTime set to continue further testing
      limiter.setNextRefillTime(limiter.getNextRefillTime() - temp);
    }
  }

  @Test
  public void testOverconsumptionAverageIntervalRefillStrategy() {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    // 10 resources are available, but we need to consume 20 resources
    // Verify that we have to wait at least 1.1sec to have 1 resource available
    assertEquals(0, limiter.getWaitIntervalMs());
    limiter.consume(20);
    // We consumed twice the quota. Need to wait 1s to get back to 0, then another 100ms for the 1
    assertEquals(1100, limiter.waitInterval(1));
    // We consumed twice the quota. Need to wait 1s to get back to 0, then another 1s to get to 10
    assertEquals(2000, limiter.waitInterval(10));

    // Verify that after 1sec we need to wait for another 0.1sec to get a resource available
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertNotEquals(0, limiter.getWaitIntervalMs(1));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 100);
    // We've waited the full 1.1sec, should now have 1 available
    assertEquals(0, limiter.getWaitIntervalMs(1));
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void testOverconsumptionFixedIntervalRefillStrategy() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter(1000, true);
    limiter.set(10, TimeUnit.SECONDS);

    // fix the current time in order to get the precise value of interval
    EnvironmentEdge edge = new EnvironmentEdge() {
      private final long ts = EnvironmentEdgeManager.currentTime();

      @Override
      public long currentTime() {
        return ts;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);
    assertEquals(0, limiter.getWaitIntervalMs());
    // 10 resources are available, but we need to consume 20 resources
    limiter.consume(20);
    // We over-consumed by 10. Since this is a fixed interval refill, where
    // each interval we refill the full limit amount, we need to wait 2 intervals:
    // first interval gets us from -10 to 0, second gets us from 0 to 10 (so we have 1+ available).
    // Base wait would be ~2000ms, but first violation gets 0.1x multiplier: 2000 * 0.1 = 200ms
    long waitInterval = limiter.waitInterval(1);
    assertTrue("Wait interval should be around 200ms (±50ms), but was: " + waitInterval, 
               150 <= waitInterval && waitInterval <= 250);
    EnvironmentEdgeManager.reset();

    // Verify that after 1sec also no resource should be available
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertNotEquals(0, limiter.getWaitIntervalMs());
    // Verify that after total 2sec the 10 resource is available
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(0, limiter.getWaitIntervalMs());
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void testFixedIntervalResourceAvailability() throws Exception {
    RateLimiter limiter = new FixedIntervalRateLimiter(1000, false);
    limiter.set(10, TimeUnit.SECONDS);

    assertEquals(0, limiter.getWaitIntervalMs(10));
    limiter.consume(3);
    assertEquals(7, limiter.getAvailable());
    assertNotEquals(0, limiter.getWaitIntervalMs(10));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(0, limiter.getWaitIntervalMs(10));
    assertEquals(10, limiter.getAvailable());
  }

  @Test
  public void testLimiterBySmallerRate() throws InterruptedException {
    // set limiter is 10 resources per seconds
    RateLimiter limiter = new FixedIntervalRateLimiter(1000, false);
    limiter.set(10, TimeUnit.SECONDS);

    int count = 0; // control the test count
    while ((count++) < 10) {
      // test will get 3 resources per 0.5 sec. so it will get 6 resources per sec.
      limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
      for (int i = 0; i < 3; i++) {
        // 6 resources/sec < limit, so limiter.canExecute(nowTs, lastTs) should be true
        assertEquals(limiter.getWaitIntervalMs(), 0);
        limiter.consume();
      }
    }
  }

  @Test
  public void testCanExecuteOfAverageIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    // when set limit is 100 per sec, this AverageIntervalRateLimiter will support at max 200 per
    // sec
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(50, testCanExecuteByRate(limiter, 50));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 100));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(200, testCanExecuteByRate(limiter, 200));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(200, testCanExecuteByRate(limiter, 500));
  }

  @Test
  public void testCanExecuteOfFixedIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter(1000, false);
    // when set limit is 100 per sec, this FixedIntervalRateLimiter will support at max 100 per sec
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(50, testCanExecuteByRate(limiter, 50));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 100));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 200));
  }

  public int testCanExecuteByRate(RateLimiter limiter, int rate) {
    int request = 0;
    int count = 0;
    while ((request++) < rate) {
      limiter.setNextRefillTime(limiter.getNextRefillTime() - limiter.getTimeUnitInMillis() / rate);
      if (limiter.getWaitIntervalMs() == 0) {
        count++;
        limiter.consume();
      }
    }
    return count;
  }

  @Test
  public void testRefillOfAverageIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(60, TimeUnit.SECONDS);
    assertEquals(60, limiter.getAvailable());
    // first refill, will return the number same with limit
    assertEquals(60, limiter.refill(limiter.getLimit()));

    limiter.consume(30);

    // after 0.2 sec, refill should return 12
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 200);
    assertEquals(12, limiter.refill(limiter.getLimit()));

    // after 0.5 sec, refill should return 30
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    assertEquals(30, limiter.refill(limiter.getLimit()));

    // after 1 sec, refill should return 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(60, limiter.refill(limiter.getLimit()));

    // after more than 1 sec, refill should return at max 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 3000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 5000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
  }

  @Test
  public void testRefillOfFixedIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter(1000, false);
    limiter.set(60, TimeUnit.SECONDS);
    assertEquals(60, limiter.getAvailable());
    // first refill, will return the number same with limit
    assertEquals(60, limiter.refill(limiter.getLimit()));

    limiter.consume(30);

    // after 0.2 sec, refill should return 0
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 200);
    assertEquals(0, limiter.refill(limiter.getLimit()));

    // after 0.5 sec, refill should return 0
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    assertEquals(0, limiter.refill(limiter.getLimit()));

    // after 1 sec, refill should return 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(60, limiter.refill(limiter.getLimit()));

    // after more than 1 sec, refill should return at max 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 3000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 5000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
  }

  @Test
  public void testUnconfiguredLimiters() throws InterruptedException {

    ManualEnvironmentEdge testEdge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(testEdge);
    long limit = Long.MAX_VALUE;

    // For unconfigured limiters, it is supposed to use as much as possible
    RateLimiter avgLimiter = new AverageIntervalRateLimiter();
    RateLimiter fixLimiter = new FixedIntervalRateLimiter(1000, false);

    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals(limit, fixLimiter.getAvailable());

    assertEquals(0, avgLimiter.getWaitIntervalMs(limit));
    avgLimiter.consume(limit);

    assertEquals(0, fixLimiter.getWaitIntervalMs(limit));
    fixLimiter.consume(limit);

    // Make sure that available is Long.MAX_VALUE
    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals(limit, fixLimiter.getAvailable());

    // after 100 millseconds, it should be able to execute limit as well
    testEdge.incValue(100);

    assertEquals(0, avgLimiter.getWaitIntervalMs(limit));
    avgLimiter.consume(limit);

    assertEquals(0, fixLimiter.getWaitIntervalMs(limit));
    fixLimiter.consume(limit);

    // Make sure that available is Long.MAX_VALUE
    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals(limit, fixLimiter.getAvailable());

    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testExtremeLimiters() throws InterruptedException {

    ManualEnvironmentEdge testEdge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(testEdge);
    long limit = Long.MAX_VALUE - 1;

    RateLimiter avgLimiter = new AverageIntervalRateLimiter();
    avgLimiter.set(limit, TimeUnit.SECONDS);
    RateLimiter fixLimiter = new FixedIntervalRateLimiter(1000, false);
    fixLimiter.set(limit, TimeUnit.SECONDS);

    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals(limit, fixLimiter.getAvailable());

    assertEquals(0, avgLimiter.getWaitIntervalMs(limit / 2));
    avgLimiter.consume(limit / 2);

    assertEquals(0, fixLimiter.getWaitIntervalMs(limit / 2));
    fixLimiter.consume(limit / 2);

    // Make sure that available is whatever left
    assertEquals((limit - (limit / 2)), avgLimiter.getAvailable());
    assertEquals((limit - (limit / 2)), fixLimiter.getAvailable());

    // after 100 millseconds, both should not be able to execute the limit
    testEdge.incValue(100);

    assertNotEquals(0, avgLimiter.getWaitIntervalMs(limit));
    assertNotEquals(0, fixLimiter.getWaitIntervalMs(limit));

    // after 500 millseconds, average interval limiter should be able to execute the limit
    testEdge.incValue(500);
    assertEquals(0, avgLimiter.getWaitIntervalMs(limit));
    assertNotEquals(0, fixLimiter.getWaitIntervalMs(limit));

    // Make sure that available is correct
    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals((limit - (limit / 2)), fixLimiter.getAvailable());

    // after 500 millseconds, both should be able to execute
    testEdge.incValue(500);
    assertEquals(0, avgLimiter.getWaitIntervalMs(limit));
    assertEquals(0, fixLimiter.getWaitIntervalMs(limit));

    // Make sure that available is Long.MAX_VALUE
    assertEquals(limit, avgLimiter.getAvailable());
    assertEquals(limit, fixLimiter.getAvailable());

    EnvironmentEdgeManager.reset();
  }

  /*
   * This test case is tricky. Basically, it simulates the following events: Thread-1 Thread-2 t0:
   * canExecute(100) and consume(100) t1: canExecute(100), avail may be increased by 80 t2:
   * consume(-80) as actual size is 20 It will check if consume(-80) can handle overflow correctly.
   */
  @Test
  public void testLimiterCompensationOverflow() throws InterruptedException {

    long limit = Long.MAX_VALUE - 1;
    long guessNumber = 100;

    // For unconfigured limiters, it is supposed to use as much as possible
    RateLimiter avgLimiter = new AverageIntervalRateLimiter();
    avgLimiter.set(limit, TimeUnit.SECONDS);

    assertEquals(limit, avgLimiter.getAvailable());

    // The initial guess is that 100 bytes.
    assertEquals(0, avgLimiter.getWaitIntervalMs(guessNumber));
    avgLimiter.consume(guessNumber);

    // Make sure that available is whatever left
    assertEquals((limit - guessNumber), avgLimiter.getAvailable());

    // Manually set avil to simulate that another thread call canExecute().
    // It is simulated by consume().
    avgLimiter.consume(-80);
    assertEquals((limit - guessNumber + 80), avgLimiter.getAvailable());

    // Now thread1 compensates 80
    avgLimiter.consume(-80);
    assertEquals(limit, avgLimiter.getAvailable());
  }

  @Test
  public void itRunsFullWithPartialRefillInterval() {
    RateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());

    // Consume the quota  
    limiter.consume(10);

    // First violation: Need to wait ~0.1s due to 0.1x multiplier on first violation (base would be ~1s)
    long waitInterval = limiter.waitInterval(10);
    assertTrue("First violation wait should be 80-120ms", 80 < waitInterval && 120 >= waitInterval);
    
    // Second violation: Linear ramp gives ~0.2x multiplier for 2nd violation
    waitInterval = limiter.waitInterval(20);
    assertTrue("Second violation wait should be 360-440ms", 360 < waitInterval && 440 >= waitInterval);

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    // We've waited the full interval, so we should now have 10
    assertEquals(0, limiter.getWaitIntervalMs(10));
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void itRunsPartialRefillIntervals() {
    RateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());

    // Consume the quota
    limiter.consume(10);

    // First violation: Need to wait ~0.1s due to 0.1x multiplier on first violation (base would be ~1s)
    long waitInterval = limiter.waitInterval(10);
    assertTrue("First violation wait should be 80-120ms", 80 < waitInterval && 120 >= waitInterval);
    
    // Second violation: Linear ramp gives ~0.2x multiplier for 2nd violation
    waitInterval = limiter.waitInterval(20);
    assertTrue("Second violation wait should be 360-440ms", 360 < waitInterval && 440 >= waitInterval);
    
    // Third violation: Linear ramp gives ~0.3x multiplier for 3rd violation, base ~100ms
    waitInterval = limiter.waitInterval(1);
    assertTrue("Third violation wait should be 25-35ms", 25 < waitInterval && 35 >= waitInterval);

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    // We've waited half the interval, so we should now have half available
    assertEquals(0, limiter.getWaitIntervalMs(5));
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void itRunsRepeatedPartialRefillIntervals() {
    RateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());
    // Consume the quota
    limiter.consume(10);
    for (int i = 0; i < 100; i++) {
      limiter.setNextRefillTime(limiter.getNextRefillTime() - 100); // free 1 resource
      limiter.consume(1);
      assertFalse(limiter.isAvailable(1)); // all resources consumed
      assertTrue(limiter.isAvailable(0)); // not negative
    }
  }

  @Test
  public void testAdaptiveWaitIntervals() {
    FixedIntervalRateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);

    // Fix current time to ensure predictable wait intervals
    EnvironmentEdge edge = new EnvironmentEdge() {
      private final long ts = EnvironmentEdgeManager.currentTime();

      @Override
      public long currentTime() {
        return ts;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    // Initially should have 0 violations
    assertEquals(0, limiter.getViolationsInCurrentInterval());

    // Verify starting state is clear
    assertEquals(0, limiter.getWaitIntervalMs());

    // Over-consume to ensure we need to wait
    limiter.consume(20);

    // First violation: should get reduced wait time (0.5x multiplier)
    long firstWaitInterval = limiter.waitInterval(1);
    assertTrue(firstWaitInterval > 0);
    assertEquals(1, limiter.getViolationsInCurrentInterval());

    // Record the base wait for comparison
    long baseWait = (long) Math.ceil(firstWaitInterval / 0.1); // Remove the 0.1 multiplier

    // Second violation: linear ramp gives 0.2x multiplier
    long secondWaitInterval = limiter.waitInterval(1);
    assertEquals(2, limiter.getViolationsInCurrentInterval());
    assertEquals((long) Math.ceil(baseWait * 0.2), secondWaitInterval);

    // Third violation: linear ramp gives 0.3x multiplier
    long thirdWaitInterval = limiter.waitInterval(1);
    assertEquals(3, limiter.getViolationsInCurrentInterval());
    assertEquals((long) Math.ceil(baseWait * 0.3), thirdWaitInterval);

    // Refill should reset violations counter
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    long refillAmount = limiter.refill(limiter.getLimit());
    assertTrue(refillAmount > 0);
    assertEquals(0, limiter.getViolationsInCurrentInterval());

    // Test with many violations to check the cap at 100x
    limiter.consume(20); // Over-consume again
    
    // Generate enough violations to hit the 100x cap
    // Exponential starts at violation 11: 1.1^(violations-10) = 100.0
    // (violations-10) * log(1.1) = log(100.0)
    // violations-10 = log(100.0)/log(1.1) ≈ 48.32, so ~49 violations after 10th
    // Total violations needed ≈ 59
    for (int i = 0; i < 59; i++) {
      limiter.waitInterval(1);
    }

    // Capture the base wait from the 59th violation (should be at cap)
    long cappedWaitInterval = limiter.waitInterval(1);
    
    // Generate one more violation - should still be at the same cap
    long stillCappedWaitInterval = limiter.waitInterval(1);
    
    // Both should be equal since we've hit the cap
    assertEquals("Wait intervals should be capped at same value", cappedWaitInterval, stillCappedWaitInterval);
    assertTrue("Capped wait interval should be substantial", cappedWaitInterval > 1000);
    
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testAdaptiveWaitIntervalReduction() {
    FixedIntervalRateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);

    // Fix current time to ensure predictable wait intervals
    EnvironmentEdge edge = new EnvironmentEdge() {
      private final long ts = EnvironmentEdgeManager.currentTime();

      @Override
      public long currentTime() {
        return ts;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    // Consume all resources and then request more than available
    assertEquals(0, limiter.getWaitIntervalMs());
    limiter.consume(20); // Over-consume by 10
    
    // This should require waiting for next refill interval plus additional time
    long firstViolationWait = limiter.waitInterval(1);
    assertTrue("First violation should have non-zero wait", firstViolationWait > 0);
    assertEquals("Should have 1 violation", 1, limiter.getViolationsInCurrentInterval());

    // Calculate what the base wait would be without the 0.1 multiplier  
    long calculatedBaseWait = (long) Math.ceil(firstViolationWait / 0.1);
    
    // Verify second violation gets 0.2x wait time (linear ramp)
    long secondViolationWait = limiter.waitInterval(1);
    assertEquals("Second violation should get 0.2x wait time", (long) Math.ceil(calculatedBaseWait * 0.2), secondViolationWait);
    assertEquals("Should have 2 violations", 2, limiter.getViolationsInCurrentInterval());
    
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testViolationCounterResetsOnRefill() {
    FixedIntervalRateLimiter limiter = new FixedIntervalRateLimiter(100, true);
    limiter.set(10, TimeUnit.SECONDS);

    // Fix current time to ensure predictable wait intervals
    EnvironmentEdge edge = new EnvironmentEdge() {
      private final long ts = EnvironmentEdgeManager.currentTime();

      @Override
      public long currentTime() {
        return ts;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    // Initially should have 0 violations
    assertEquals("Should start with 0 violations", 0, limiter.getViolationsInCurrentInterval());

    // Verify starting state is clear
    assertEquals(0, limiter.getWaitIntervalMs());
    
    // Over-consume to put limiter in negative state, ensuring we need to wait
    limiter.consume(20); // Over-consume by 10
    
    // Now try to get resources - this should require waiting and generate violations
    long wait1 = limiter.waitInterval(1); // First violation
    assertTrue("First wait should be non-zero", wait1 > 0);
    assertEquals("Should have 1 violation", 1, limiter.getViolationsInCurrentInterval());
    
    long wait2 = limiter.waitInterval(1); // Second violation
    assertTrue("Second wait should be non-zero", wait2 > 0);
    assertEquals("Should have 2 violations", 2, limiter.getViolationsInCurrentInterval());
    
    long wait3 = limiter.waitInterval(1); // Third violation
    assertTrue("Third wait should be non-zero", wait3 > 0);
    assertEquals("Should have 3 violations", 3, limiter.getViolationsInCurrentInterval());
    
    // Advance time to trigger refill
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    long refillAmount = limiter.refill(limiter.getLimit());
    assertTrue("Should have refilled", refillAmount > 0);
    
    // Violations counter should be reset after refill
    assertEquals("Violations should reset on refill", 0, limiter.getViolationsInCurrentInterval());
    
    // Over-consume again and create a new violation
    limiter.consume(20); // Over-consume again
    long waitInterval = limiter.waitInterval(1);
    assertTrue("Should have non-zero wait", waitInterval > 0);
    assertEquals("Should have 1 violation in new interval", 1, limiter.getViolationsInCurrentInterval());
    
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testAdaptiveWaitIntervalsDisabled() {
    FixedIntervalRateLimiter limiter = new FixedIntervalRateLimiter(100, false);
    limiter.set(10, TimeUnit.SECONDS);

    // Fix current time to ensure predictable wait intervals
    EnvironmentEdge edge = new EnvironmentEdge() {
      private final long ts = EnvironmentEdgeManager.currentTime();

      @Override
      public long currentTime() {
        return ts;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    // Initially should have 0 violations
    assertEquals(0, limiter.getViolationsInCurrentInterval());

    // Verify starting state is clear
    assertEquals(0, limiter.getWaitIntervalMs());

    // Over-consume to ensure we need to wait
    limiter.consume(20);

    // With adaptive disabled, all wait intervals should be the same base wait time
    long firstWaitInterval = limiter.waitInterval(1);
    assertTrue(firstWaitInterval > 0);
    
    long secondWaitInterval = limiter.waitInterval(1);
    long thirdWaitInterval = limiter.waitInterval(1);
    
    // All wait intervals should be identical when adaptive is disabled
    assertEquals("All wait intervals should be the same when adaptive is disabled", 
                 firstWaitInterval, secondWaitInterval);
    assertEquals("All wait intervals should be the same when adaptive is disabled", 
                 secondWaitInterval, thirdWaitInterval);
    
    // Violations counter should still increment even when adaptive is disabled
    assertEquals(3, limiter.getViolationsInCurrentInterval());
    
    EnvironmentEdgeManager.reset();
  }
}
