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
package com.timecho.influxdb2tsfile.support;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.schema.IMeasurementSchema;

public final class TsFileRows {

  private TsFileRows() {}

  public static List<Map<String, Object>> read(File file, String table) throws Exception {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build()) {
      TableSchema schema = null;
      for (TableSchema candidate : reader.getAllTableSchema()) {
        if (candidate.getTableName().equals(table)) {
          schema = candidate;
        }
      }
      if (schema == null) {
        throw new IllegalStateException("table " + table + " not found in " + file);
      }
      List<String> columns = new ArrayList<>();
      for (IMeasurementSchema column : schema.getColumnSchemas()) {
        columns.add(column.getMeasurementName());
      }
      try (ResultSet resultSet =
          reader.query(table, columns, Long.MIN_VALUE, Long.MAX_VALUE)) {
        ResultSetMetadata metadata = resultSet.getMetadata();
        while (resultSet.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("Time", resultSet.getLong(1));
          for (int i = 0; i < columns.size(); i++) {
            int index = i + 2;
            if (resultSet.isNull(index)) {
              row.put(columns.get(i), null);
              continue;
            }
            TSDataType type = metadata.getColumnType(index);
            switch (type) {
              case BOOLEAN:
                row.put(columns.get(i), resultSet.getBoolean(index));
                break;
              case INT32:
              case INT64:
                row.put(columns.get(i), resultSet.getLong(index));
                break;
              case FLOAT:
              case DOUBLE:
                row.put(columns.get(i), resultSet.getDouble(index));
                break;
              default:
                row.put(columns.get(i), resultSet.getString(index));
                break;
            }
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }
}
