/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.avro.schemas;

@SuppressWarnings("all")
public class ComplexRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ComplexRecord\",\"namespace\":\"org.apache.samza.sql.avro.schemas\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"doc\":\"Record id.\",\"default\":null},{\"name\":\"bool_value\",\"type\":[\"null\",\"boolean\"],\"doc\":\"Boolean Value.\",\"default\":null},{\"name\":\"double_value\",\"type\":[\"null\",\"double\"],\"doc\":\"double Value.\",\"default\":null},{\"name\":\"float_value\",\"type\":[\"null\",\"float\"],\"doc\":\"float Value.\",\"default\":null},{\"name\":\"string_value\",\"type\":[\"null\",\"string\"],\"doc\":\"string Value.\",\"default\":null},{\"name\":\"bytes_value\",\"type\":[\"null\",\"bytes\"],\"doc\":\"bytes Value.\",\"default\":null},{\"name\":\"long_value\",\"type\":[\"null\",\"long\"],\"doc\":\"long Value.\",\"default\":null},{\"name\":\"fixed_value\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"MyFixed\",\"size\":16}],\"doc\":\"fixed Value.\"},{\"name\":\"array_values\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"doc\":\"array values in the record.\",\"default\":[]},{\"name\":\"map_values\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"doc\":\"map values in the record.\",\"default\":[]},{\"name\":\"enum_value\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"TestEnumType\",\"symbols\":[\"foo\",\"bar\"]}],\"doc\":\"enum value.\",\"default\":[]},{\"name\":\"empty_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"emptySubRecord\",\"fields\":[]}]},{\"name\":\"array_records\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SubRecord\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"doc\":\"sub record id\"},{\"name\":\"sub_values\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"Sub record \"}]}],\"doc\":\"array of records.\",\"default\":[]},{\"name\":\"union_value\",\"type\":[\"null\",\"SubRecord\",\"string\"],\"doc\":\"union Value.\",\"default\":null}]}");
  /** Record id. */
  public java.lang.Integer id;
  /** Boolean Value. */
  public java.lang.Boolean bool_value;
  /** double Value. */
  public java.lang.Double double_value;
  /** float Value. */
  public java.lang.Float float_value;
  /** string Value. */
  public java.lang.CharSequence string_value;
  /** bytes Value. */
  public java.nio.ByteBuffer bytes_value;
  /** long Value. */
  public java.lang.Long long_value;
  /** fixed Value. */
  public org.apache.samza.sql.avro.schemas.MyFixed fixed_value;
  /** array values in the record. */
  public java.util.List<java.lang.CharSequence> array_values;
  /** map values in the record. */
  public java.util.Map<java.lang.CharSequence,java.lang.CharSequence> map_values;
  /** enum value. */
  public org.apache.samza.sql.avro.schemas.TestEnumType enum_value;
  public org.apache.samza.sql.avro.schemas.emptySubRecord empty_record;
  /** array of records. */
  public org.apache.samza.sql.avro.schemas.SubRecord array_records;
  /** union Value. */
  public java.lang.Object union_value;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return bool_value;
    case 2: return double_value;
    case 3: return float_value;
    case 4: return string_value;
    case 5: return bytes_value;
    case 6: return long_value;
    case 7: return fixed_value;
    case 8: return array_values;
    case 9: return map_values;
    case 10: return enum_value;
    case 11: return empty_record;
    case 12: return array_records;
    case 13: return union_value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    case 1: bool_value = (java.lang.Boolean)value$; break;
    case 2: double_value = (java.lang.Double)value$; break;
    case 3: float_value = (java.lang.Float)value$; break;
    case 4: string_value = (java.lang.CharSequence)value$; break;
    case 5: bytes_value = (java.nio.ByteBuffer)value$; break;
    case 6: long_value = (java.lang.Long)value$; break;
    case 7: fixed_value = (org.apache.samza.sql.avro.schemas.MyFixed)value$; break;
    case 8: array_values = (java.util.List<java.lang.CharSequence>)value$; break;
    case 9: map_values = (java.util.Map<java.lang.CharSequence,java.lang.CharSequence>)value$; break;
    case 10: enum_value = (org.apache.samza.sql.avro.schemas.TestEnumType)value$; break;
    case 11: empty_record = (org.apache.samza.sql.avro.schemas.emptySubRecord)value$; break;
    case 12: array_records = (org.apache.samza.sql.avro.schemas.SubRecord)value$; break;
    case 13: union_value = (java.lang.Object)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
