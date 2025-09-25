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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestRpcRetryingCallerImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcRetryingCallerImpl.class);

  @Test
  public void itUsesSpecialPauseForCQTBE() throws Exception {
    itUsesSpecialPauseForServerOverloaded(CallQueueTooBigException.class);
  }

  @Test
  public void itUsesSpecialPauseForCDE() throws Exception {
    itUsesSpecialPauseForServerOverloaded(CallDroppedException.class);
  }

  private void itUsesSpecialPauseForServerOverloaded(
    Class<? extends HBaseServerException> exceptionClass) throws Exception {

    // the actual values don't matter here as long as they're distinct.
    // the ThrowingCallable will assert that the passed in pause from RpcRetryingCallerImpl
    // matches the specialPauseMillis
    long pauseMillis = 1;
    long specialPauseMillis = 2;

    RpcRetryingCallerImpl<Void> caller = new RpcRetryingCallerImpl<>(pauseMillis,
      specialPauseMillis, 2, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, 0, 0, null);

    RetryingCallable<Void> callable =
      new ThrowingCallable(CallQueueTooBigException.class, specialPauseMillis);
    try {
      caller.callWithRetries(callable, 5000);
      fail("Expected " + exceptionClass.getSimpleName());
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof HBaseServerException);
    }
  }

  private static class ThrowingCallable implements RetryingCallable<Void> {
    private final Class<? extends HBaseServerException> exceptionClass;
    private final long specialPauseMillis;

    public ThrowingCallable(Class<? extends HBaseServerException> exceptionClass,
      long specialPauseMillis) {
      this.exceptionClass = exceptionClass;
      this.specialPauseMillis = specialPauseMillis;
    }

    @Override
    public void prepare(boolean reload) throws IOException {

    }

    @Override
    public void throwable(Throwable t, boolean retrying) {

    }

    @Override
    public String getExceptionMessageAdditionalDetail() {
      return null;
    }

    @Override
    public long sleep(long pause, int tries) {
      assertEquals(pause, specialPauseMillis);
      return 0;
    }

    @Override
    public Void call(int callTimeout) throws Exception {
      throw exceptionClass.getConstructor().newInstance();
    }
  }

  @Test
  public void itCallsInterceptorLifecycleInCallWithoutRetries() throws Exception {
    RetryingCallerInterceptor mockInterceptor = mock(RetryingCallerInterceptor.class);
    RetryingCallerInterceptorContext mockContext = mock(RetryingCallerInterceptorContext.class);
    RetryingCallable<String> mockCallable = mock(RetryingCallable.class);

    when(mockInterceptor.createEmptyContext()).thenReturn(mockContext);
    when(mockContext.prepare(mockCallable, 0)).thenReturn(mockContext);
    when(mockCallable.call(1000)).thenReturn("success");

    RpcRetryingCallerImpl<String> caller =
      new RpcRetryingCallerImpl<>(100, 200, 3, mockInterceptor, 0, 0, null);

    String result = caller.callWithoutRetries(mockCallable, 1000);

    assertEquals("success", result);
    verify(mockCallable).prepare(false);
    verify(mockContext).setOperationStartTime(anyLong());
    verify(mockContext).setRpcStartTime(anyLong());
    verify(mockContext).setAttemptNumber(0);
    verify(mockInterceptor).intercept(mockContext);
    verify(mockInterceptor, never()).handleFailure(mockContext, null);
    verify(mockInterceptor, never()).updateFailureInfo(mockContext);
  }

  @Test
  public void itHandlesIOExceptionInCallWithoutRetries() throws Exception {
    RetryingCallerInterceptor mockInterceptor = mock(RetryingCallerInterceptor.class);
    RetryingCallerInterceptorContext mockContext = mock(RetryingCallerInterceptorContext.class);
    RetryingCallable<String> mockCallable = mock(RetryingCallable.class);
    IOException testException = new IOException("test failure");

    when(mockInterceptor.createEmptyContext()).thenReturn(mockContext);
    when(mockContext.prepare(mockCallable, 0)).thenReturn(mockContext);
    when(mockCallable.call(1000)).thenThrow(testException);

    RpcRetryingCallerImpl<String> caller =
      new RpcRetryingCallerImpl<>(100, 200, 3, mockInterceptor, 0, 0, null);

    try {
      caller.callWithoutRetries(mockCallable, 1000);
      fail("Expected IOException");
    } catch (IOException e) {
      assertSame(testException, e);
    }

    verify(mockInterceptor).handleFailure(mockContext, testException);
    verify(mockInterceptor).updateFailureInfo(mockContext);
  }

  @Test
  public void itPreservesPreemptiveFastFailExceptionInCallWithoutRetries() throws Exception {
    RetryingCallerInterceptor mockInterceptor = mock(RetryingCallerInterceptor.class);
    RetryingCallerInterceptorContext mockContext = mock(RetryingCallerInterceptorContext.class);
    RetryingCallable<String> mockCallable = mock(RetryingCallable.class);
    PreemptiveFastFailException testException = new PreemptiveFastFailException("fast fail");

    when(mockInterceptor.createEmptyContext()).thenReturn(mockContext);
    when(mockContext.prepare(mockCallable, 0)).thenReturn(mockContext);
    when(mockCallable.call(1000)).thenThrow(testException);

    RpcRetryingCallerImpl<String> caller =
      new RpcRetryingCallerImpl<>(100, 200, 3, mockInterceptor, 0, 0, null);

    try {
      caller.callWithoutRetries(mockCallable, 1000);
      fail("Expected PreemptiveFastFailException");
    } catch (PreemptiveFastFailException e) {
      assertSame(testException, e);
    }

    verify(mockInterceptor, never()).handleFailure(mockContext, testException);
    verify(mockInterceptor, never()).updateFailureInfo(mockContext);
  }

  @Test
  public void itHandlesRetriesExhaustedWithDetailsExceptionInCallWithoutRetries() throws Exception {
    RetryingCallerInterceptor mockInterceptor = mock(RetryingCallerInterceptor.class);
    RetryingCallerInterceptorContext mockContext = mock(RetryingCallerInterceptorContext.class);
    RetryingCallable<String> mockCallable = mock(RetryingCallable.class);

    // Create REWDE with some mock data
    List<Throwable> causes = Arrays.asList(new IOException("cause1"), new IOException("cause2"));
    List<Row> actions = Arrays.asList(mock(Row.class), mock(Row.class));
    List<String> hostnames = Arrays.asList("host1:123", "host2:456");
    RetriesExhaustedWithDetailsException rewde =
      new RetriesExhaustedWithDetailsException(causes, actions, hostnames);

    when(mockInterceptor.createEmptyContext()).thenReturn(mockContext);
    when(mockContext.prepare(mockCallable, 0)).thenReturn(mockContext);
    when(mockCallable.call(1000)).thenThrow(rewde);

    RpcRetryingCallerImpl<String> caller =
      new RpcRetryingCallerImpl<>(100, 200, 3, mockInterceptor, 0, 0, null);

    try {
      caller.callWithoutRetries(mockCallable, 1000);
      fail("Expected RetriesExhaustedWithDetailsException");
    } catch (RetriesExhaustedWithDetailsException e) {
      assertSame(rewde, e);
    }

    verify(mockContext).setBatchFailures(causes);
    verify(mockInterceptor).handleFailure(mockContext, rewde);
    verify(mockInterceptor).updateFailureInfo(mockContext);
  }

  @Test
  public void itHandlesInterceptorThrowingPreemptiveFastFailInCallWithoutRetries()
    throws Exception {
    RetryingCallerInterceptor mockInterceptor = mock(RetryingCallerInterceptor.class);
    RetryingCallerInterceptorContext mockContext = mock(RetryingCallerInterceptorContext.class);
    RetryingCallable<String> mockCallable = mock(RetryingCallable.class);
    IOException originalException = new IOException("original");
    PreemptiveFastFailException fastFailException =
      new PreemptiveFastFailException("interceptor fast fail");

    when(mockInterceptor.createEmptyContext()).thenReturn(mockContext);
    when(mockContext.prepare(mockCallable, 0)).thenReturn(mockContext);
    when(mockCallable.call(1000)).thenThrow(originalException);
    when(mockInterceptor.handleFailure(mockContext, originalException))
      .thenThrow(fastFailException);

    RpcRetryingCallerImpl<String> caller =
      new RpcRetryingCallerImpl<>(100, 200, 3, mockInterceptor, 0, 0, null);

    try {
      caller.callWithoutRetries(mockCallable, 1000);
      fail("Expected PreemptiveFastFailException from interceptor");
    } catch (PreemptiveFastFailException e) {
      assertSame(fastFailException, e);
    }

    verify(mockInterceptor).updateFailureInfo(mockContext);
  }
}
