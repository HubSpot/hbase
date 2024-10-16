/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift2.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.client.TableDescriptor
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2022-07-05")
public class TTableDescriptor implements org.apache.thrift.TBase<TTableDescriptor, TTableDescriptor._Fields>, java.io.Serializable, Cloneable, Comparable<TTableDescriptor> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TTableDescriptor");

  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tableName", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField COLUMNS_FIELD_DESC = new org.apache.thrift.protocol.TField("columns", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField ATTRIBUTES_FIELD_DESC = new org.apache.thrift.protocol.TField("attributes", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField DURABILITY_FIELD_DESC = new org.apache.thrift.protocol.TField("durability", org.apache.thrift.protocol.TType.I32, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TTableDescriptorStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TTableDescriptorTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable TTableName tableName; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<TColumnFamilyDescriptor> columns; // optional
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.nio.ByteBuffer> attributes; // optional
  /**
   * 
   * @see TDurability
   */
  public @org.apache.thrift.annotation.Nullable TDurability durability; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_NAME((short)1, "tableName"),
    COLUMNS((short)2, "columns"),
    ATTRIBUTES((short)3, "attributes"),
    /**
     * 
     * @see TDurability
     */
    DURABILITY((short)4, "durability");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TABLE_NAME
          return TABLE_NAME;
        case 2: // COLUMNS
          return COLUMNS;
        case 3: // ATTRIBUTES
          return ATTRIBUTES;
        case 4: // DURABILITY
          return DURABILITY;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.COLUMNS,_Fields.ATTRIBUTES,_Fields.DURABILITY};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_NAME, new org.apache.thrift.meta_data.FieldMetaData("tableName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TTableName.class)));
    tmpMap.put(_Fields.COLUMNS, new org.apache.thrift.meta_data.FieldMetaData("columns", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TColumnFamilyDescriptor.class))));
    tmpMap.put(_Fields.ATTRIBUTES, new org.apache.thrift.meta_data.FieldMetaData("attributes", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    tmpMap.put(_Fields.DURABILITY, new org.apache.thrift.meta_data.FieldMetaData("durability", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TDurability.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TTableDescriptor.class, metaDataMap);
  }

  public TTableDescriptor() {
  }

  public TTableDescriptor(
    TTableName tableName)
  {
    this();
    this.tableName = tableName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TTableDescriptor(TTableDescriptor other) {
    if (other.isSetTableName()) {
      this.tableName = new TTableName(other.tableName);
    }
    if (other.isSetColumns()) {
      java.util.List<TColumnFamilyDescriptor> __this__columns = new java.util.ArrayList<TColumnFamilyDescriptor>(other.columns.size());
      for (TColumnFamilyDescriptor other_element : other.columns) {
        __this__columns.add(new TColumnFamilyDescriptor(other_element));
      }
      this.columns = __this__columns;
    }
    if (other.isSetAttributes()) {
      java.util.Map<java.nio.ByteBuffer,java.nio.ByteBuffer> __this__attributes = new java.util.HashMap<java.nio.ByteBuffer,java.nio.ByteBuffer>(other.attributes);
      this.attributes = __this__attributes;
    }
    if (other.isSetDurability()) {
      this.durability = other.durability;
    }
  }

  public TTableDescriptor deepCopy() {
    return new TTableDescriptor(this);
  }

  @Override
  public void clear() {
    this.tableName = null;
    this.columns = null;
    this.attributes = null;
    this.durability = null;
  }

  @org.apache.thrift.annotation.Nullable
  public TTableName getTableName() {
    return this.tableName;
  }

  public TTableDescriptor setTableName(@org.apache.thrift.annotation.Nullable TTableName tableName) {
    this.tableName = tableName;
    return this;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  /** Returns true if field tableName is set (has been assigned a value) and false otherwise */
  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public void setTableNameIsSet(boolean value) {
    if (!value) {
      this.tableName = null;
    }
  }

  public int getColumnsSize() {
    return (this.columns == null) ? 0 : this.columns.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TColumnFamilyDescriptor> getColumnsIterator() {
    return (this.columns == null) ? null : this.columns.iterator();
  }

  public void addToColumns(TColumnFamilyDescriptor elem) {
    if (this.columns == null) {
      this.columns = new java.util.ArrayList<TColumnFamilyDescriptor>();
    }
    this.columns.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TColumnFamilyDescriptor> getColumns() {
    return this.columns;
  }

  public TTableDescriptor setColumns(@org.apache.thrift.annotation.Nullable java.util.List<TColumnFamilyDescriptor> columns) {
    this.columns = columns;
    return this;
  }

  public void unsetColumns() {
    this.columns = null;
  }

  /** Returns true if field columns is set (has been assigned a value) and false otherwise */
  public boolean isSetColumns() {
    return this.columns != null;
  }

  public void setColumnsIsSet(boolean value) {
    if (!value) {
      this.columns = null;
    }
  }

  public int getAttributesSize() {
    return (this.attributes == null) ? 0 : this.attributes.size();
  }

  public void putToAttributes(java.nio.ByteBuffer key, java.nio.ByteBuffer val) {
    if (this.attributes == null) {
      this.attributes = new java.util.HashMap<java.nio.ByteBuffer,java.nio.ByteBuffer>();
    }
    this.attributes.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.nio.ByteBuffer,java.nio.ByteBuffer> getAttributes() {
    return this.attributes;
  }

  public TTableDescriptor setAttributes(@org.apache.thrift.annotation.Nullable java.util.Map<java.nio.ByteBuffer,java.nio.ByteBuffer> attributes) {
    this.attributes = attributes;
    return this;
  }

  public void unsetAttributes() {
    this.attributes = null;
  }

  /** Returns true if field attributes is set (has been assigned a value) and false otherwise */
  public boolean isSetAttributes() {
    return this.attributes != null;
  }

  public void setAttributesIsSet(boolean value) {
    if (!value) {
      this.attributes = null;
    }
  }

  /**
   * 
   * @see TDurability
   */
  @org.apache.thrift.annotation.Nullable
  public TDurability getDurability() {
    return this.durability;
  }

  /**
   * 
   * @see TDurability
   */
  public TTableDescriptor setDurability(@org.apache.thrift.annotation.Nullable TDurability durability) {
    this.durability = durability;
    return this;
  }

  public void unsetDurability() {
    this.durability = null;
  }

  /** Returns true if field durability is set (has been assigned a value) and false otherwise */
  public boolean isSetDurability() {
    return this.durability != null;
  }

  public void setDurabilityIsSet(boolean value) {
    if (!value) {
      this.durability = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((TTableName)value);
      }
      break;

    case COLUMNS:
      if (value == null) {
        unsetColumns();
      } else {
        setColumns((java.util.List<TColumnFamilyDescriptor>)value);
      }
      break;

    case ATTRIBUTES:
      if (value == null) {
        unsetAttributes();
      } else {
        setAttributes((java.util.Map<java.nio.ByteBuffer,java.nio.ByteBuffer>)value);
      }
      break;

    case DURABILITY:
      if (value == null) {
        unsetDurability();
      } else {
        setDurability((TDurability)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_NAME:
      return getTableName();

    case COLUMNS:
      return getColumns();

    case ATTRIBUTES:
      return getAttributes();

    case DURABILITY:
      return getDurability();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TABLE_NAME:
      return isSetTableName();
    case COLUMNS:
      return isSetColumns();
    case ATTRIBUTES:
      return isSetAttributes();
    case DURABILITY:
      return isSetDurability();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TTableDescriptor)
      return this.equals((TTableDescriptor)that);
    return false;
  }

  public boolean equals(TTableDescriptor that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_columns = true && this.isSetColumns();
    boolean that_present_columns = true && that.isSetColumns();
    if (this_present_columns || that_present_columns) {
      if (!(this_present_columns && that_present_columns))
        return false;
      if (!this.columns.equals(that.columns))
        return false;
    }

    boolean this_present_attributes = true && this.isSetAttributes();
    boolean that_present_attributes = true && that.isSetAttributes();
    if (this_present_attributes || that_present_attributes) {
      if (!(this_present_attributes && that_present_attributes))
        return false;
      if (!this.attributes.equals(that.attributes))
        return false;
    }

    boolean this_present_durability = true && this.isSetDurability();
    boolean that_present_durability = true && that.isSetDurability();
    if (this_present_durability || that_present_durability) {
      if (!(this_present_durability && that_present_durability))
        return false;
      if (!this.durability.equals(that.durability))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetTableName()) ? 131071 : 524287);
    if (isSetTableName())
      hashCode = hashCode * 8191 + tableName.hashCode();

    hashCode = hashCode * 8191 + ((isSetColumns()) ? 131071 : 524287);
    if (isSetColumns())
      hashCode = hashCode * 8191 + columns.hashCode();

    hashCode = hashCode * 8191 + ((isSetAttributes()) ? 131071 : 524287);
    if (isSetAttributes())
      hashCode = hashCode * 8191 + attributes.hashCode();

    hashCode = hashCode * 8191 + ((isSetDurability()) ? 131071 : 524287);
    if (isSetDurability())
      hashCode = hashCode * 8191 + durability.getValue();

    return hashCode;
  }

  @Override
  public int compareTo(TTableDescriptor other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetTableName(), other.isSetTableName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableName, other.tableName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetColumns(), other.isSetColumns());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumns()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columns, other.columns);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetAttributes(), other.isSetAttributes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAttributes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.attributes, other.attributes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDurability(), other.isSetDurability());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDurability()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.durability, other.durability);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TTableDescriptor(");
    boolean first = true;

    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (isSetColumns()) {
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
    }
    if (isSetAttributes()) {
      if (!first) sb.append(", ");
      sb.append("attributes:");
      if (this.attributes == null) {
        sb.append("null");
      } else {
        sb.append(this.attributes);
      }
      first = false;
    }
    if (isSetDurability()) {
      if (!first) sb.append(", ");
      sb.append("durability:");
      if (this.durability == null) {
        sb.append("null");
      } else {
        sb.append(this.durability);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (tableName == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tableName' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (tableName != null) {
      tableName.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TTableDescriptorStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TTableDescriptorStandardScheme getScheme() {
      return new TTableDescriptorStandardScheme();
    }
  }

  private static class TTableDescriptorStandardScheme extends org.apache.thrift.scheme.StandardScheme<TTableDescriptor> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TTableDescriptor struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.tableName = new TTableName();
              struct.tableName.read(iprot);
              struct.setTableNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // COLUMNS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list162 = iprot.readListBegin();
                struct.columns = new java.util.ArrayList<TColumnFamilyDescriptor>(_list162.size);
                @org.apache.thrift.annotation.Nullable TColumnFamilyDescriptor _elem163;
                for (int _i164 = 0; _i164 < _list162.size; ++_i164)
                {
                  _elem163 = new TColumnFamilyDescriptor();
                  _elem163.read(iprot);
                  struct.columns.add(_elem163);
                }
                iprot.readListEnd();
              }
              struct.setColumnsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ATTRIBUTES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map165 = iprot.readMapBegin();
                struct.attributes = new java.util.HashMap<java.nio.ByteBuffer,java.nio.ByteBuffer>(2*_map165.size);
                @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key166;
                @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _val167;
                for (int _i168 = 0; _i168 < _map165.size; ++_i168)
                {
                  _key166 = iprot.readBinary();
                  _val167 = iprot.readBinary();
                  struct.attributes.put(_key166, _val167);
                }
                iprot.readMapEnd();
              }
              struct.setAttributesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DURABILITY
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.durability = org.apache.hadoop.hbase.thrift2.generated.TDurability.findByValue(iprot.readI32());
              struct.setDurabilityIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TTableDescriptor struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        struct.tableName.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.columns != null) {
        if (struct.isSetColumns()) {
          oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.columns.size()));
            for (TColumnFamilyDescriptor _iter169 : struct.columns)
            {
              _iter169.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.attributes != null) {
        if (struct.isSetAttributes()) {
          oprot.writeFieldBegin(ATTRIBUTES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.attributes.size()));
            for (java.util.Map.Entry<java.nio.ByteBuffer, java.nio.ByteBuffer> _iter170 : struct.attributes.entrySet())
            {
              oprot.writeBinary(_iter170.getKey());
              oprot.writeBinary(_iter170.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.durability != null) {
        if (struct.isSetDurability()) {
          oprot.writeFieldBegin(DURABILITY_FIELD_DESC);
          oprot.writeI32(struct.durability.getValue());
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TTableDescriptorTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TTableDescriptorTupleScheme getScheme() {
      return new TTableDescriptorTupleScheme();
    }
  }

  private static class TTableDescriptorTupleScheme extends org.apache.thrift.scheme.TupleScheme<TTableDescriptor> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TTableDescriptor struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.tableName.write(oprot);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetColumns()) {
        optionals.set(0);
      }
      if (struct.isSetAttributes()) {
        optionals.set(1);
      }
      if (struct.isSetDurability()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetColumns()) {
        {
          oprot.writeI32(struct.columns.size());
          for (TColumnFamilyDescriptor _iter171 : struct.columns)
          {
            _iter171.write(oprot);
          }
        }
      }
      if (struct.isSetAttributes()) {
        {
          oprot.writeI32(struct.attributes.size());
          for (java.util.Map.Entry<java.nio.ByteBuffer, java.nio.ByteBuffer> _iter172 : struct.attributes.entrySet())
          {
            oprot.writeBinary(_iter172.getKey());
            oprot.writeBinary(_iter172.getValue());
          }
        }
      }
      if (struct.isSetDurability()) {
        oprot.writeI32(struct.durability.getValue());
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TTableDescriptor struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.tableName = new TTableName();
      struct.tableName.read(iprot);
      struct.setTableNameIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list173 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.columns = new java.util.ArrayList<TColumnFamilyDescriptor>(_list173.size);
          @org.apache.thrift.annotation.Nullable TColumnFamilyDescriptor _elem174;
          for (int _i175 = 0; _i175 < _list173.size; ++_i175)
          {
            _elem174 = new TColumnFamilyDescriptor();
            _elem174.read(iprot);
            struct.columns.add(_elem174);
          }
        }
        struct.setColumnsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map176 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
          struct.attributes = new java.util.HashMap<java.nio.ByteBuffer,java.nio.ByteBuffer>(2*_map176.size);
          @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _key177;
          @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _val178;
          for (int _i179 = 0; _i179 < _map176.size; ++_i179)
          {
            _key177 = iprot.readBinary();
            _val178 = iprot.readBinary();
            struct.attributes.put(_key177, _val178);
          }
        }
        struct.setAttributesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.durability = org.apache.hadoop.hbase.thrift2.generated.TDurability.findByValue(iprot.readI32());
        struct.setDurabilityIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

