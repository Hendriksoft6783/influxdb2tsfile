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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.timecho.influxdb2tsfile.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

public final class TableSpec {

  private final String measurement;
  private final String tableName;
  private final List<String> tagKeys;
  private final List<String> tagColumns;
  private final List<String> fieldKeys;
  private final List<String> fieldColumns;
  private final List<TSDataType> fieldTypes;
  private final Map<String, Integer> fieldIndexByKey;
  private final Map<String, String> sourceKeyByColumn;

  public TableSpec(
      String measurement,
      String tableName,
      List<String> tagKeys,
      List<String> tagColumns,
      List<String> fieldKeys,
      List<String> fieldColumns,
      List<TSDataType> fieldTypes) {
    this.measurement = measurement;
    this.tableName = tableName;
    this.tagKeys = Collections.unmodifiableList(new ArrayList<>(tagKeys));
    this.tagColumns = Collections.unmodifiableList(new ArrayList<>(tagColumns));
    this.fieldKeys = Collections.unmodifiableList(new ArrayList<>(fieldKeys));
    this.fieldColumns = Collections.unmodifiableList(new ArrayList<>(fieldColumns));
    this.fieldTypes = Collections.unmodifiableList(new ArrayList<>(fieldTypes));
    this.fieldIndexByKey = new HashMap<>();
    for (int i = 0; i < fieldKeys.size(); i++) {
      fieldIndexByKey.put(fieldKeys.get(i), i);
    }
    Map<String, String> mapping = new LinkedHashMap<>();
    for (int i = 0; i < tagKeys.size(); i++) {
      mapping.put(tagColumns.get(i), tagKeys.get(i));
    }
    for (int i = 0; i < fieldKeys.size(); i++) {
      mapping.put(fieldColumns.get(i), fieldKeys.get(i));
    }
    this.sourceKeyByColumn = Collections.unmodifiableMap(mapping);
  }

  public String measurement() {
    return measurement;
  }

  public String tableName() {
    return tableName;
  }

  public List<String> tagKeys() {
    return tagKeys;
  }

  public List<String> tagColumns() {
    return tagColumns;
  }

  public List<String> fieldKeys() {
    return fieldKeys;
  }

  public List<String> fieldColumns() {
    return fieldColumns;
  }

  public List<TSDataType> fieldTypes() {
    return fieldTypes;
  }

  public Map<String, String> sourceKeyByColumn() {
    return sourceKeyByColumn;
  }

  public int tagCount() {
    return tagKeys.size();
  }

  public int fieldCount() {
    return fieldKeys.size();
  }

  public int columnCount() {
    return tagKeys.size() + fieldKeys.size();
  }

  public List<String> columnNames() {
    List<String> names = new ArrayList<>(columnCount());
    names.addAll(tagColumns);
    names.addAll(fieldColumns);
    return names;
  }

  public List<TSDataType> columnTypes() {
    List<TSDataType> types = new ArrayList<>(columnCount());
    for (int i = 0; i < tagColumns.size(); i++) {
      types.add(TSDataType.STRING);
    }
    types.addAll(fieldTypes);
    return types;
  }

  public List<ColumnCategory> columnCategories() {
    List<ColumnCategory> categories = new ArrayList<>(columnCount());
    for (int i = 0; i < tagColumns.size(); i++) {
      categories.add(ColumnCategory.TAG);
    }
    for (int i = 0; i < fieldColumns.size(); i++) {
      categories.add(ColumnCategory.FIELD);
    }
    return categories;
  }

  public int fieldIndex(String fieldKey) {
    Integer index = fieldIndexByKey.get(fieldKey);
    return index == null ? -1 : index;
  }

  public TSDataType fieldType(int index) {
    return fieldTypes.get(index);
  }

  public String[] tagValues(Map<String, String> seriesTags) {
    String[] values = new String[tagKeys.size()];
    for (int i = 0; i < tagKeys.size(); i++) {
      String value = seriesTags.get(tagKeys.get(i));
      values[i] = value == null ? "" : value;
    }
    return values;
  }

  public TableSchema toTableSchema(CompressionType compression) {
    List<IMeasurementSchema> schemas = new ArrayList<>(columnCount());
    for (int i = 0; i < tagColumns.size(); i++) {
      schemas.add(measurementSchema(tagColumns.get(i), TSDataType.STRING, compression));
    }
    for (int i = 0; i < fieldColumns.size(); i++) {
      schemas.add(measurementSchema(fieldColumns.get(i), fieldTypes.get(i), compression));
    }
    return new TableSchema(tableName, schemas, columnCategories());
  }

  private static IMeasurementSchema measurementSchema(
      String name, TSDataType type, CompressionType compression) {
    if (compression == null) {
      return new MeasurementSchema(name, type);
    }
    return new MeasurementSchema(name, type, defaultEncoding(type), compression);
  }

  private static TSEncoding defaultEncoding(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return TSEncoding.RLE;
      case INT32:
      case INT64:
        return TSEncoding.TS_2DIFF;
      case FLOAT:
      case DOUBLE:
        return TSEncoding.GORILLA;
      default:
        return TSEncoding.PLAIN;
    }
  }

  public Tablet newTablet(int rows) {
    return new Tablet(tableName, columnNames(), columnTypes(), columnCategories(), rows);
  }

  public Object coerce(int fieldIndex, Object value) {
    if (value == null) {
      return null;
    }
    TSDataType type = fieldTypes.get(fieldIndex);
    String column = fieldColumns.get(fieldIndex);
    try {
      switch (type) {
        case STRING:
        case TEXT:
        case BLOB:
          return value instanceof String ? value : String.valueOf(value);
        case BOOLEAN:
          if (value instanceof Boolean) {
            return value;
          }
          if (value instanceof String) {
            String text = ((String) value).trim();
            if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("t") || text.equals("1")) {
              return Boolean.TRUE;
            }
            if (text.equalsIgnoreCase("false") || text.equalsIgnoreCase("f") || text.equals("0")) {
              return Boolean.FALSE;
            }
          }
          throw new DataConversionException("cannot convert value '" + value + "' to BOOLEAN");
        case DOUBLE:
          if (value instanceof Double) {
            return value;
          }
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(String.valueOf(value).trim());
        case INT64:
        case INT32:
          if (value instanceof Long) {
            return value;
          }
          if (value instanceof Integer) {
            return ((Integer) value).longValue();
          }
          if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d)) {
              return (long) d;
            }
            throw new DataConversionException("cannot convert decimal value " + value + " to INT64");
          }
          return Long.parseLong(String.valueOf(value).trim());
        default:
          throw new DataConversionException("unsupported column type " + type);
      }
    } catch (NumberFormatException e) {
      throw new DataConversionException(
          "cannot convert value '" + value + "' of column '" + column + "' to " + type);
    }
  }

  @Override
  public String toString() {
    return "TableSpec{table=" + tableName + ", tags=" + tagColumns + ", fields=" + fieldColumns + "}";
  }
}
