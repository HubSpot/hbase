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
package org.apache.hadoop.hbase.client.coprocessor;

import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.getParsedGenericInstance;
import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.validateArgAndGetPB;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This client class is for invoking the aggregate functions deployed on the Region Server side via
 * the AggregateService. This class will implement the supporting functionality for
 * summing/processing the individual results obtained from the AggregateService for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate functions. For all
 * aggregate functions,
 * <ul>
 * <li>start row &lt; end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are provided, an IOException
 * will be thrown. An optional column qualifier can also be defined.</li>
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the parameter type. For
 * average and std, it returns a double value. For row count, it returns a long value.</li>
 * </ul>
 * <p>
 * Call {@link #close()} when done.
 */
@InterfaceAudience.Public
public class AggregationClient implements Closeable {
  // TODO: This class is not used. Move to examples?
  private static final Logger log = LoggerFactory.getLogger(AggregationClient.class);
  private final Connection connection;
  private final boolean manageConnection;

  /**
   * An RpcController implementation for use here in this endpoint.
   */
  static class AggregationClientRpcController implements RpcController {
    private String errorText;
    private boolean cancelled = false;
    private boolean failed = false;

    @Override
    public String errorText() {
      return this.errorText;
    }

    @Override
    public boolean failed() {
      return this.failed;
    }

    @Override
    public boolean isCanceled() {
      return this.cancelled;
    }

    @Override
    public void notifyOnCancel(RpcCallback<Object> arg0) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      this.errorText = null;
      this.cancelled = false;
      this.failed = false;
    }

    @Override
    public void setFailed(String errorText) {
      this.failed = true;
      this.errorText = errorText;
    }

    @Override
    public void startCancel() {
      this.cancelled = true;
    }
  }

  /**
   * Creates AggregationClient with no underlying Connection. Users of this constructor should limit
   * themselves to methods here which take a {@link Table} argument, such as
   * {@link #rowCount(Table, ColumnInterpreter, Scan)}. Use of methods which instead take a
   * TableName, such as {@link #rowCount(TableName, ColumnInterpreter, Scan)}, will throw an
   * IOException.
   */
  public AggregationClient() {
    this(null, false);
  }

  /**
   * Creates AggregationClient using the passed in Connection, which will be used by methods taking
   * a {@link TableName} to create the necessary {@link Table} for the call. The Connection is
   * externally managed by the caller and will not be closed if {@link #close()} is called. There is
   * no need to call {@link #close()} for AggregationClients created this way.
   * @param connection the connection to use
   */
  public AggregationClient(Connection connection) {
    this(connection, false);
  }

  /**
   * Creates AggregationClient with internally managed Connection, which will be used by methods
   * taking a {@link TableName} to create the necessary {@link Table} for the call. The Connection
   * will immediately be created will be closed when {@link #close()} is called. It's important to
   * call {@link #close()} when done with this AggregationClient and to otherwise treat it as a
   * shared Singleton.
   * @param cfg Configuration to use to create connection
   */
  public AggregationClient(Configuration cfg) {
    // Create a connection on construction. Will use it making each of the calls below.
    this(createConnection(cfg), true);
  }

  private static Connection createConnection(Configuration cfg) {
    try {
      return ConnectionFactory.createConnection(cfg);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private AggregationClient(Connection connection, boolean manageConnection) {
    this.connection = connection;
    this.manageConnection = manageConnection;
  }

  @Override
  public void close() throws IOException {
    if (manageConnection && this.connection != null && !this.connection.isClosed()) {
      this.connection.close();
    }
  }

  // visible for tests
  boolean isClosed() {
    return manageConnection && this.connection != null && this.connection.isClosed();
  }

  private Connection getConnection() throws IOException {
    if (connection == null) {
      throw new IOException(
        "Connection not initialized. Use the correct constructor, or use the methods taking a Table");
    }
    return connection;
  }

  /**
   * It gives the maximum value of a column for a given column family for the given range. In case
   * qualifier is null, a max of all values for the given family is returned.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return max val &lt;R&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R
    max(final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return max(table, ci, scan);
    }
  }

  /**
   * It gives the maximum value of a column for a given column family for the given range. In case
   * qualifier is null, a max of all values for the given family is returned.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return max val &lt;&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R max(final Table table,
    final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    class MaxCallBack implements Batch.Callback<R> {
      R max = null;

      R getMax() {
        return max;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        max = (max == null || (result != null && ci.compare(max, result) < 0)) ? result : max;
      }
    }
    MaxCallBack aMaxCallBack = new MaxCallBack();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
        R max = null;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getMax(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }
          if (response.getFirstPartCount() > 0) {
            ByteString b = response.getFirstPart(0);
            Q q = getParsedGenericInstance(ci.getClass(), 3, b);
            R result = ci.getCellValueFromProto(q);
            max = (max == null || (result != null && ci.compare(max, result) < 0)) ? result : max;
          }
          postScan(response, partialResultContext);
        }
        return max;
      }, aMaxCallBack);
    return aMaxCallBack.getMax();
  }

  /**
   * It gives the minimum value of a column for a given column family for the given range. In case
   * qualifier is null, a min of all values for the given family is returned.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return min val &lt;R&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R
    min(final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return min(table, ci, scan);
    }
  }

  /**
   * It gives the minimum value of a column for a given column family for the given range. In case
   * qualifier is null, a min of all values for the given family is returned.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return min val &lt;R&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R min(final Table table,
    final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    class MinCallBack implements Batch.Callback<R> {
      private R min = null;

      public R getMinimum() {
        return min;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        min = (min == null || (result != null && ci.compare(result, min) < 0)) ? result : min;
      }
    }

    MinCallBack minCallBack = new MinCallBack();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        R min = null;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getMin(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }
          if (response.getFirstPartCount() > 0) {
            ByteString b = response.getFirstPart(0);
            Q q = getParsedGenericInstance(ci.getClass(), 3, b);
            R result = ci.getCellValueFromProto(q);
            min = (min == null || (result != null && ci.compare(result, min) < 0)) ? result : min;
          }
          postScan(response, partialResultContext);
        }
        return min;
      }, minCallBack);
    log.debug("Min fom all regions is: " + minCallBack.getMinimum());
    return minCallBack.getMinimum();
  }

  /**
   * It gives the row count, by summing up the individual results obtained from regions. In case the
   * qualifier is null, FirstKeyValueFilter is used to optimised the operation. In case qualifier is
   * provided, I can't use the filter as it may set the flag to skip to next row, but the value read
   * is not of the given filter: in this case, this particular row will not be counted ==&gt; an
   * error.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> long
    rowCount(final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return rowCount(table, ci, scan);
    }
  }

  /**
   * It gives the row count, by summing up the individual results obtained from regions. In case the
   * qualifier is null, FirstKeyValueFilter is used to optimised the operation. In case qualifier is
   * provided, I can't use the filter as it may set the flag to skip to next row, but the value read
   * is not of the given filter: in this case, this particular row will not be counted ==&gt; an
   * error.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> long
    rowCount(final Table table, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, true);
    class RowNumCallback implements Batch.Callback<Long> {
      private final AtomicLong rowCountL = new AtomicLong(0);

      public long getRowNumCount() {
        return rowCountL.get();
      }

      @Override
      public void update(byte[] region, byte[] row, Long result) {
        rowCountL.addAndGet(result);
      }
    }

    RowNumCallback rowNum = new RowNumCallback();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        long sum = 0;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getRowNum(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }
          byte[] bytes = getBytesFromResponse(response.getFirstPart(0));
          ByteBuffer bb = ByteBuffer.allocate(8).put(bytes);
          bb.rewind();
          sum += bb.getLong();
          postScan(response, partialResultContext);
        }
        log.debug("GetRowNum finished with sum {}", sum);
        return sum;
      }, rowNum);
    return rowNum.getRowNumCount();
  }

  /**
   * It sums up the value returned from various regions. In case qualifier is null, summation of all
   * the column qualifiers in the given family is done.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return sum &lt;S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> S
    sum(final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return sum(table, ci, scan);
    }
  }

  /**
   * It sums up the value returned from various regions. In case qualifier is null, summation of all
   * the column qualifiers in the given family is done.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return sum &lt;S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> S sum(final Table table,
    final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);

    class SumCallBack implements Batch.Callback<S> {
      S sumVal = null;

      public S getSumResult() {
        return sumVal;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, S result) {
        sumVal = ci.add(sumVal, result);
      }
    }
    SumCallBack sumCallBack = new SumCallBack();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        // Not sure what is going on here why I have to do these casts. TODO.
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        S sum = null;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getSum(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }
          if (response.getFirstPartCount() > 0) {
            ByteString b = response.getFirstPart(0);
            T t = getParsedGenericInstance(ci.getClass(), 4, b);
            sum = ci.add(sum, ci.getPromotedValueFromProto(t));
          }
          postScan(response, partialResultContext);
        }
        return sum;
      }, sumCallBack);
    return sumCallBack.getSumResult();
  }

  /**
   * It computes average while fetching sum and row count from all the corresponding regions.
   * Approach is to compute a global sum of region level sum and rowcount and then compute the
   * average.
   * @param tableName the name of the table to scan
   * @param scan      the HBase scan object to use to read data from HBase
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  private <R, S, P extends Message, Q extends Message, T extends Message> Pair<S, Long> getAvgArgs(
    final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
    throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return getAvgArgs(table, ci, scan);
    }
  }

  /**
   * It computes average while fetching sum and row count from all the corresponding regions.
   * Approach is to compute a global sum of region level sum and rowcount and then compute the
   * average.
   * @param table table to scan.
   * @param scan  the HBase scan object to use to read data from HBase
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  private <R, S, P extends Message, Q extends Message, T extends Message> Pair<S, Long>
    getAvgArgs(final Table table, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    class AvgCallBack implements Batch.Callback<Pair<S, Long>> {
      S sum = null;
      Long rowCount = 0L;

      public synchronized Pair<S, Long> getAvgArgs() {
        return new Pair<>(sum, rowCount);
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<S, Long> result) {
        sum = ci.add(sum, result.getFirst());
        rowCount += result.getSecond();
      }
    }

    AvgCallBack avgCallBack = new AvgCallBack();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        S sum = null;
        long rowCount = 0L;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getAvg(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }
          if (response.getFirstPartCount() > 0) {
            ByteString b = response.getFirstPart(0);
            T t = getParsedGenericInstance(ci.getClass(), 4, b);
            sum = ci.add(sum, ci.getPromotedValueFromProto(t));
            ByteBuffer bb =
              ByteBuffer.allocate(8).put(getBytesFromResponse(response.getSecondPart()));
            bb.rewind();
            rowCount += bb.getLong();
          }
          postScan(response, partialResultContext);
        }
        return new Pair<>(sum, rowCount);
      }, avgCallBack);
    return avgCallBack.getAvgArgs();
  }

  /**
   * This is the client side interface/handle for calling the average method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the average and returs the double value.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> double
    avg(final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, Scan scan)
      throws Throwable {
    Pair<S, Long> p = getAvgArgs(tableName, ci, scan);
    return ci.divideForAvg(p.getFirst(), p.getSecond());
  }

  /**
   * This is the client side interface/handle for calling the average method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the average and returs the double value.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> double
    avg(final Table table, final ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    Pair<S, Long> p = getAvgArgs(table, ci, scan);
    return ci.divideForAvg(p.getFirst(), p.getSecond());
  }

  /**
   * It computes a global standard deviation for a given column and its value. Standard deviation is
   * square root of (average of squares - average*average). From individual regions, it obtains sum,
   * square sum and number of rows. With these, the above values are computed to get the global std.
   * @param table table to scan.
   * @param scan  the HBase scan object to use to read data from HBase
   * @return standard deviations
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  private <R, S, P extends Message, Q extends Message, T extends Message> Pair<List<S>, Long>
    getStdArgs(final Table table, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    class StdCallback implements Batch.Callback<Pair<List<S>, Long>> {
      long rowCountVal = 0L;
      S sumVal = null, sumSqVal = null;

      public synchronized Pair<List<S>, Long> getStdParams() {
        List<S> l = new ArrayList<>(2);
        l.add(sumVal);
        l.add(sumSqVal);
        Pair<List<S>, Long> p = new Pair<>(l, rowCountVal);
        return p;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<List<S>, Long> result) {
        if (result.getFirst().size() > 0) {
          sumVal = ci.add(sumVal, result.getFirst().get(0));
          sumSqVal = ci.add(sumSqVal, result.getFirst().get(1));
          rowCountVal += result.getSecond();
        }
      }
    }

    StdCallback stdCallback = new StdCallback();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        long rowCountVal = 0L;
        S sumVal = null, sumSqVal = null;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getStd(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }

          if (response.getFirstPartCount() == 2) {
            ByteString b = response.getFirstPart(0);
            T t = getParsedGenericInstance(ci.getClass(), 4, b);
            sumVal = ci.add(sumVal, ci.getPromotedValueFromProto(t));

            b = response.getFirstPart(1);
            t = getParsedGenericInstance(ci.getClass(), 4, b);
            sumSqVal = ci.add(sumSqVal, ci.getPromotedValueFromProto(t));

            ByteBuffer bb =
              ByteBuffer.allocate(8).put(getBytesFromResponse(response.getSecondPart()));
            bb.rewind();
            rowCountVal += bb.getLong();
          }
          postScan(response, partialResultContext);
        }
        return new Pair<>(Arrays.asList(sumVal, sumSqVal), rowCountVal);
      }, stdCallback);
    return stdCallback.getStdParams();
  }

  /**
   * This is the client side interface/handle for calling the std method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the std and returns the double value.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> double std(
    final TableName tableName, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return std(table, ci, scan);
    }
  }

  /**
   * This is the client side interface/handle for calling the std method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the std and returns the double value.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return &lt;R, S&gt;
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> double
    std(final Table table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    Pair<List<S>, Long> p = getStdArgs(table, ci, scan);
    double avg = ci.divideForAvg(p.getFirst().get(0), p.getSecond());
    double avgOfSumSq = ci.divideForAvg(p.getFirst().get(1), p.getSecond());
    double res = avgOfSumSq - avg * avg; // variance
    res = Math.pow(res, 0.5);
    return res;
  }

  /**
   * It helps locate the region with median for a given column whose weight is specified in an
   * optional column. From individual regions, it obtains sum of values and sum of weights.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return pair whose first element is a map between start row of the region and (sum of values,
   *         sum of weights) for the region, the second element is (sum of values, sum of weights)
   *         for all the regions chosen
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  private <R, S, P extends Message, Q extends Message, T extends Message>
    Pair<NavigableMap<byte[], List<S>>, List<S>> getMedianArgs(final Table table,
      final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    final NavigableMap<byte[], List<S>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    class StdCallback implements Batch.Callback<List<S>> {
      S sumVal = null, sumWeights = null;

      public synchronized Pair<NavigableMap<byte[], List<S>>, List<S>> getMedianParams() {
        List<S> l = new ArrayList<>(2);
        l.add(sumVal);
        l.add(sumWeights);
        Pair<NavigableMap<byte[], List<S>>, List<S>> p = new Pair<>(map, l);
        return p;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, List<S> result) {
        map.put(row, result);
        sumVal = ci.add(sumVal, result.get(0));
        sumWeights = ci.add(sumWeights, result.get(1));
      }
    }
    StdCallback stdCallback = new StdCallback();
    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
      instance -> {
        RpcController controller = new AggregationClientRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<AggregateResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<>();
        S sumVal = null, sumWeights = null;
        PartialResultContext partialResultContext = new PartialResultContext(requestArg.getScan());
        while (partialResultContext.hasMore) {
          instance.getMedian(controller, partialResultContext.withCurrentScan(requestArg), rpcCallback);
          AggregateResponse response = rpcCallback.get();
          if (controller.failed()) {
            throw new IOException(controller.errorText());
          }

          if (response.getFirstPartCount() == 2) {
            ByteString b = response.getFirstPart(0);
            T t = getParsedGenericInstance(ci.getClass(), 4, b);
            sumVal = ci.add(sumVal, ci.getPromotedValueFromProto(t));

            b = response.getFirstPart(1);
            t = getParsedGenericInstance(ci.getClass(), 4, b);
            sumWeights = ci.add(sumWeights, ci.getPromotedValueFromProto(t));
          }
          postScan(response, partialResultContext);
        }
        return Arrays.asList(sumVal, sumWeights);
      }, stdCallback);
    return stdCallback.getMedianParams();
  }

  /**
   * This is the client side interface/handler for calling the median method for a given cf-cq
   * combination. This method collects the necessary parameters to compute the median and returns
   * the median.
   * @param tableName the name of the table to scan
   * @param ci        the user's ColumnInterpreter implementation
   * @param scan      the HBase scan object to use to read data from HBase
   * @return R the median
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R median(
    final TableName tableName, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    try (Table table = getConnection().getTable(tableName)) {
      return median(table, ci, scan);
    }
  }

  /**
   * This is the client side interface/handler for calling the median method for a given cf-cq
   * combination. This method collects the necessary parameters to compute the median and returns
   * the median.
   * @param table table to scan.
   * @param ci    the user's ColumnInterpreter implementation
   * @param scan  the HBase scan object to use to read data from HBase
   * @return R the median
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &amp;
   *                   propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> R median(final Table table,
    ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    Pair<NavigableMap<byte[], List<S>>, List<S>> p = getMedianArgs(table, ci, scan);
    byte[] startRow = null;
    byte[] colFamily = scan.getFamilies()[0];
    NavigableSet<byte[]> quals = scan.getFamilyMap().get(colFamily);
    NavigableMap<byte[], List<S>> map = p.getFirst();
    S sumVal = p.getSecond().get(0);
    S sumWeights = p.getSecond().get(1);
    double halfSumVal = ci.divideForAvg(sumVal, 2L);
    double movingSumVal = 0;
    boolean weighted = false;
    if (quals.size() > 1) {
      weighted = true;
      halfSumVal = ci.divideForAvg(sumWeights, 2L);
    }

    for (Map.Entry<byte[], List<S>> entry : map.entrySet()) {
      S s = weighted ? entry.getValue().get(1) : entry.getValue().get(0);
      double newSumVal = movingSumVal + ci.divideForAvg(s, 1L);
      if (newSumVal > halfSumVal) {
        // we found the region with the median
        break;
      }
      movingSumVal = newSumVal;
      startRow = entry.getKey();
    }
    // scan the region with median and find it
    Scan scan2 = new Scan(scan);
    // inherit stop row from method parameter
    if (startRow != null) {
      scan2.setStartRow(startRow);
    }
    ResultScanner scanner = null;
    try {
      int cacheSize = scan2.getCaching();
      if (!scan2.getCacheBlocks() || scan2.getCaching() < 2) {
        scan2.setCacheBlocks(true);
        cacheSize = 5;
        scan2.setCaching(cacheSize);
      }
      scanner = table.getScanner(scan2);
      Result[] results = null;
      byte[] qualifier = quals.pollFirst();
      // qualifier for the weight column
      byte[] weightQualifier = weighted ? quals.pollLast() : qualifier;
      R value = null;
      do {
        results = scanner.next(cacheSize);
        if (results != null && results.length > 0) {
          for (int i = 0; i < results.length; i++) {
            Result r = results[i];
            // retrieve weight
            Cell kv = r.getColumnLatestCell(colFamily, weightQualifier);
            R newValue = ci.getValue(colFamily, weightQualifier, kv);
            S s = ci.castToReturnType(newValue);
            double newSumVal = movingSumVal + ci.divideForAvg(s, 1L);
            // see if we have moved past the median
            if (newSumVal > halfSumVal) {
              return value;
            }
            movingSumVal = newSumVal;
            kv = r.getColumnLatestCell(colFamily, qualifier);
            value = ci.getValue(colFamily, qualifier, kv);
          }
        }
      } while (results != null && results.length > 0);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return null;
  }

  byte[] getBytesFromResponse(ByteString response) {
    return response.toByteArray();
  }

  private static final class PartialResultContext {
    ClientProtos.Scan currentScan;
    boolean hasMore = true;

    public PartialResultContext(ClientProtos.Scan currentScan) {
      this.currentScan = currentScan;
    }

    public AggregateRequest withCurrentScan(AggregateRequest request) {
      return request.toBuilder()
        .setScan(currentScan).build();
    }
  }

  private void postScan(AggregateResponse response, PartialResultContext context) {
    // Will be false for servers that do not support paging in AggregateImplementation
    if (response.hasNextScan()) {
      context.currentScan = response.getNextScan();
    } else {
      context.hasMore = false;
    }
    if (response.hasWaitIntervalMs()) {
      try {
        log.debug("Sleeping {}ms for requested wait interval", response.getWaitIntervalMs());
        Thread.sleep(response.getWaitIntervalMs());
      } catch (InterruptedException ignored) {}
    }
  }
}
