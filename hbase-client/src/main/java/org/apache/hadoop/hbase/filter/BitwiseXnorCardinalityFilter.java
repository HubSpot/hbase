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
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Performs a bitwise XNOR over the rowkey substring starting at {@code offset} vs. the provided
 * {@code sig}. The filter returns the cardinality value in a synthetic cell {@code 0:cardinality}.
 * Note that this cell must exist in the row in order for the Filter to overwrite its value, so when
 * inserting into the index, include that qualifier in your Put with an empty value.
 */
@InterfaceAudience.Public
public class BitwiseXnorCardinalityFilter extends FilterBase {
  private static final byte[] CARDINALITY_FAM = Bytes.toBytes("0");
  private static final byte[] CARDINALITY_QUAL = Bytes.toBytes("cardinality");

  private final int offset;
  private final int length;
  private final BitSet sig;
  private final ExtendedCellBuilder builder;

  private int cardinality = 0;

  public BitwiseXnorCardinalityFilter(int offset, boolean[] sig) {
    this.offset = offset;
    this.length = sig.length;
    this.sig = new BitSet(length);
    for (int i = 0; i < sig.length; i++) {
      if (sig[i]) {
        this.sig.set(i);
      }
    }
    this.builder = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);
  }

  public BitwiseXnorCardinalityFilter(int offset, byte[] sig) {
    this.offset = offset;
    this.sig = BitSet.valueOf(sig);
    this.length = this.sig.length();
    this.builder = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    if (offset + length > cell.getRowLength()) {
      throw new IOException(BitwiseXnorCardinalityFilter.class.getSimpleName() + "(offset=" + offset
        + ",length=" + length + ") " + " cannot be applied to rowkey of length " + length);
    }

    // n.b. the similarity score for SuperBit computed by summing the number position where the
    // values are equal between the two signatures. We compute that value "cheaply" using BitSets
    // by taking the Exclusive NOR of the two signatures, that is, ~(sig1 ^ sig2).
    // See also,
    // https://github.com/tdebatty/java-LSH/blob/v0.12/src/main/java/info/debatty/java/lsh/SuperBit.java#L197
    // https://en.wikipedia.org/wiki/XNOR_gate

    ByteBuffer buf = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    buf.position(offset);
    BitSet cloneSig = (BitSet) sig.clone();
    BitSet cellSig = BitSet.valueOf(buf);
    cloneSig.xor(cellSig);
    cloneSig.flip(0, cloneSig.length());
    cardinality = cloneSig.cardinality();

    return cardinality == 0;
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    if (
      !(CellUtil.matchingFamily(v, CARDINALITY_FAM)
        && CellUtil.matchingQualifier(v, CARDINALITY_QUAL))
    ) {
      return v;
    }

    // write out a new cell for `cardinality` that contains the sum of set similarity
    builder.setRow(v.getRowArray(), v.getRowOffset(), v.getRowLength())
      .setFamily(v.getFamilyArray(), v.getFamilyOffset(), v.getFamilyLength())
      .setQualifier(v.getQualifierArray(), v.getQualifierOffset(), v.getQualifierLength())
      .setType(Cell.Type.Put).setValue(Bytes.toBytes(cardinality));

    if (v instanceof ExtendedCell) {
      ExtendedCell extendedCell = (ExtendedCell) v;
      builder.setTags(extendedCell.getTagsArray(), extendedCell.getTagsOffset(),
        extendedCell.getTagsLength());
    }
    return builder.build();
  }

  @Override
  public void reset() throws IOException {
    cardinality = 0;
    builder.clear();
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return FilterProtos.BitwiseXnorCardinalityFilter.newBuilder()
      .setSig(ByteString.copyFrom(sig.toByteArray())).setOffset(offset).build().toByteArray();
  }

  public static BitwiseXnorCardinalityFilter parseFrom(final byte[] pbBytes)
    throws DeserializationException {
    FilterProtos.BitwiseXnorCardinalityFilter proto;
    try {
      proto = FilterProtos.BitwiseXnorCardinalityFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    // Just return a new instance.
    return new BitwiseXnorCardinalityFilter(proto.getOffset(), proto.getSig().toByteArray());
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BitwiseXnorCardinalityFilter that = (BitwiseXnorCardinalityFilter) o;
    return offset == that.offset && length == that.length && cardinality == that.cardinality
      && Objects.equals(sig, that.sig);
  }

}
