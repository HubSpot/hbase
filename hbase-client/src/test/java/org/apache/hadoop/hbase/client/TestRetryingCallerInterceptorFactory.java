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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestRetryingCallerInterceptorFactory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRetryingCallerInterceptorFactory.class);

  public static class TestInterceptor extends RetryingCallerInterceptor {
    public TestInterceptor(Configuration conf) {
      // Test interceptor
    }

    @Override
    public RetryingCallerInterceptorContext createEmptyContext() {
      return new NoOpRetryingInterceptorContext();
    }

    @Override
    public void handleFailure(RetryingCallerInterceptorContext context, Throwable t)
      throws IOException {
      // Test implementation
    }

    @Override
    public void intercept(RetryingCallerInterceptorContext abstractRetryingCallerInterceptorContext)
      throws IOException {
      // Test implementation
    }

    @Override
    public void updateFailureInfo(RetryingCallerInterceptorContext context) {
      // Test implementation
    }

    @Override
    public String toString() {
      return "TestInterceptor";
    }
  }

  @Test
  public void itBuildsDefaultNoOpInterceptor() {
    Configuration conf = new Configuration();
    RetryingCallerInterceptorFactory factory = new RetryingCallerInterceptorFactory(conf);

    RetryingCallerInterceptor interceptor = factory.build();
    assertEquals(RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, interceptor);
  }

  @Test
  public void itBuildsFastFailInterceptorWhenEnabled() {
    Configuration conf = new Configuration();
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    RetryingCallerInterceptorFactory factory = new RetryingCallerInterceptorFactory(conf);

    RetryingCallerInterceptor interceptor = factory.build();
    assertTrue(interceptor instanceof PreemptiveFastFailInterceptor);
  }

  @Test
  public void itBuildsCustomInterceptorWhenConfigured() {
    Configuration conf = new Configuration();
    conf.set(HConstants.HBASE_CLIENT_RETRYING_CALLER_INTERCEPTOR_IMPL,
      TestInterceptor.class.getName());
    RetryingCallerInterceptorFactory factory = new RetryingCallerInterceptorFactory(conf);

    RetryingCallerInterceptor interceptor = factory.build();
    assertTrue(interceptor instanceof TestInterceptor);
  }

  @Test
  public void itFallsBackToDefaultWhenCustomInterceptorFails() {
    Configuration conf = new Configuration();
    conf.set(HConstants.HBASE_CLIENT_RETRYING_CALLER_INTERCEPTOR_IMPL, "nonexistent.class.Name");
    RetryingCallerInterceptorFactory factory = new RetryingCallerInterceptorFactory(conf);

    RetryingCallerInterceptor interceptor = factory.build();
    assertEquals(RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, interceptor);
  }

  @Test
  public void itPrefersCustomOverFastFail() {
    Configuration conf = new Configuration();
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.set(HConstants.HBASE_CLIENT_RETRYING_CALLER_INTERCEPTOR_IMPL,
      TestInterceptor.class.getName());
    RetryingCallerInterceptorFactory factory = new RetryingCallerInterceptorFactory(conf);

    RetryingCallerInterceptor interceptor = factory.build();
    assertTrue(interceptor instanceof TestInterceptor);
  }
}
