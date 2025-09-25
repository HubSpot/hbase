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

import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory implementation to provide the {@link ConnectionImplementation} with the implementation of
 * the {@link RetryingCallerInterceptor} that we would use to intercept the
 * {@link RpcRetryingCaller} during the course of their calls.
 */

@InterfaceAudience.Private
class RetryingCallerInterceptorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RetryingCallerInterceptorFactory.class);
  private Configuration conf;
  private final boolean failFast;
  public static final RetryingCallerInterceptor NO_OP_INTERCEPTOR =
    new NoOpRetryableCallerInterceptor(null);

  public RetryingCallerInterceptorFactory(Configuration conf) {
    this.conf = conf;
    failFast = conf.getBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED,
      HConstants.HBASE_CLIENT_ENABLE_FAST_FAIL_MODE_DEFAULT);
  }

  /**
   * This builds the implementation of {@link RetryingCallerInterceptor} that we specify in the conf
   * and returns the same. Configuration priority: 1. HBASE_CLIENT_RETRYING_CALLER_INTERCEPTOR_IMPL
   * - custom interceptor (highest priority) 2. HBASE_CLIENT_ENABLE_FAST_FAIL_MODE - fast-fail
   * interceptor (if enabled) 3. No-op interceptor (default)
   * @return The factory build method which creates the {@link RetryingCallerInterceptor} object
   *         according to the configuration.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "REC_CATCH_EXCEPTION",
      justification = "Convert thrown exception to unchecked")
  public RetryingCallerInterceptor build() {
    // First priority: check for custom interceptor implementation
    String customInterceptorClass =
      conf.get(HConstants.HBASE_CLIENT_RETRYING_CALLER_INTERCEPTOR_IMPL);
    if (customInterceptorClass != null && !customInterceptorClass.trim().isEmpty()) {
      try {
        Class<?> c = Class.forName(customInterceptorClass.trim());
        Constructor<?> constructor = c.getDeclaredConstructor(Configuration.class);
        constructor.setAccessible(true);
        RetryingCallerInterceptor ret = (RetryingCallerInterceptor) constructor.newInstance(conf);
        LOG.info("Using custom RetryingCallerInterceptor: {}", customInterceptorClass);
        LOG.trace("Using {} for intercepting the RpcRetryingCaller", ret);
        return ret;
      } catch (Exception e) {
        LOG.warn("Failed to instantiate custom interceptor: {}, falling back to default behavior",
          customInterceptorClass, e);
      }
    }

    // Second priority: use fast-fail interceptor if enabled, otherwise no-op
    RetryingCallerInterceptor ret = buildDefaultInterceptor();
    LOG.trace("Using {} for intercepting the RpcRetryingCaller", ret);
    return ret;
  }

  private RetryingCallerInterceptor buildDefaultInterceptor() {
    if (failFast) {
      try {
        Class<?> c = conf.getClass(HConstants.HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL,
          PreemptiveFastFailInterceptor.class);
        Constructor<?> constructor = c.getDeclaredConstructor(Configuration.class);
        constructor.setAccessible(true);
        return (RetryingCallerInterceptor) constructor.newInstance(conf);
      } catch (Exception e) {
        return new PreemptiveFastFailInterceptor(conf);
      }
    }
    return NO_OP_INTERCEPTOR;
  }
}
