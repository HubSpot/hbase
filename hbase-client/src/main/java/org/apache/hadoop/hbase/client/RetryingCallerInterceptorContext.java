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

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The context object used in the {@link RpcRetryingCaller} to enable
 * {@link RetryingCallerInterceptor} to intercept calls. {@link RetryingCallerInterceptorContext} is
 * the piece of information unique to a retrying call that transfers information from the call into
 * the {@link RetryingCallerInterceptor} so that {@link RetryingCallerInterceptor} can take
 * appropriate action according to the specific logic
 */
@InterfaceAudience.Private
abstract class RetryingCallerInterceptorContext {

  protected long operationStartTime = -1;
  protected long rpcStartTime = -1;
  protected int attemptNumber = 0;
  protected List<Throwable> batchFailures = null;

  protected RetryingCallerInterceptorContext() {
  }

  /**
   * This function clears the internal state of the context object.
   */
  public abstract void clear();

  /**
   * This prepares the context object by populating it with information specific to the
   * implementation of the {@link RetryingCallerInterceptor} along with which this will be used. :
   * The {@link RetryingCallable} that contains the information about the call that is being made.
   * @return A new {@link RetryingCallerInterceptorContext} object that can be used for use in the
   *         current retrying call
   */
  public abstract RetryingCallerInterceptorContext prepare(RetryingCallable<?> callable);

  /**
   * Telescopic extension that takes which of the many retries we are currently in. : The
   * {@link RetryingCallable} that contains the information about the call that is being made. : The
   * retry number that we are currently in.
   * @return A new context object that can be used for use in the current retrying call
   */
  public abstract RetryingCallerInterceptorContext prepare(RetryingCallable<?> callable, int tries);

  /**
   * Returns the time when the operation (combo of all RPCs in a given get, put, multiget) started,
   * in milliseconds since epoch
   */
  public long getOperationStartTime() {
    return operationStartTime;
  }

  /**
   * Set the time when the operation started
   * @param operationStartTime the operation start time in milliseconds since epoch
   */
  public void setOperationStartTime(long operationStartTime) {
    this.operationStartTime = operationStartTime;
  }

  /** Returns the time when the current RPC started, in milliseconds since epoch */
  public long getRpcStartTime() {
    return rpcStartTime;
  }

  /**
   * Set the time when the current RPC started
   * @param rpcStartTime the RPC start time in milliseconds since epoch
   */
  public void setRpcStartTime(long rpcStartTime) {
    this.rpcStartTime = rpcStartTime;
  }

  /** Returns the attempt number for the current RPC in the operation (0-based) */
  public int getAttemptNumber() {
    return attemptNumber;
  }

  /**
   * Set the attempt number for the current RPC
   * @param attemptNumber the attempt number (0-based)
   */
  public void setAttemptNumber(int attemptNumber) {
    this.attemptNumber = attemptNumber;
  }

  /**
   * Returns the list of individual failures for batch operations, or null if not a batch operation
   */
  public List<Throwable> getBatchFailures() {
    return batchFailures;
  }

  /**
   * Set the batch failures for batch operations
   * @param batchFailures the list of individual failures
   */
  public void setBatchFailures(List<Throwable> batchFailures) {
    this.batchFailures = batchFailures;
  }
}
