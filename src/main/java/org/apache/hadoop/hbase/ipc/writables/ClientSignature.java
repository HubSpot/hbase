package org.apache.hadoop.hbase.ipc.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClientSignature implements Writable {
  private String signature;
  public ClientSignature() {
    this(null);
  }

  public ClientSignature(String signature) {
    this.signature = signature;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(signature);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.signature = dataInput.readUTF();
  }
}
