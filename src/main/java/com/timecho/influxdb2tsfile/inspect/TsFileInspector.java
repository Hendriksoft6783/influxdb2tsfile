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
package com.timecho.influxdb2tsfile.inspect;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.timecho.influxdb2tsfile.core.Times;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TsFileInspector {

  private static final Logger LOG = LoggerFactory.getLogger(TsFileInspector.class);

  private TsFileInspector() {}

  public static FileInspection inspect(Path path, int sampleRows) throws IOException {
    FileInspection inspection = new FileInspection();
    inspection.file = path.getFileName().toString();
    try {
      inspection.bytes = Files.size(path);
    } catch (IOException e) {
      inspection.bytes = 0;
    }
    File file = path.toFile();
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build()) {
      List<TableSchema> schemas = reader.getAllTableSchema();
      for (TableSchema schema : schemas) {
        FileInspection.TableInspection table = new FileInspection.TableInspection();
        table.table = schema.getTableName();
        List<String> columnNames = new ArrayList<>();
        List<IMeasurementSchema> columnSchemas = schema.getColumnSchemas();
        List<ColumnCategory> categories = schema.getColumnTypes();
        for (int i = 0; i < columnSchemas.size(); i++) {
          IMeasurementSchema column = columnSchemas.get(i);
          ColumnCategory category = i < categories.size() ? categories.get(i) : ColumnCategory.FIELD;
          columnNames.add(column.getMeasurementName());
          table.columns.add(
              new FileInspection.ColumnInspection(
                  column.getMeasurementName(), column.getType().name(), category.name()));
        }
        if (columnNames.isEmpty()) {
          inspection.tables.add(table);
          continue;
        }
        Set<String> devices = new HashSet<>();
        List<Integer> tagIndexes = new ArrayList<>();
        for (int i = 0; i < table.columns.size(); i++) {
          if ("TAG".equals(table.columns.get(i).category)) {
            tagIndexes.add(i);
          }
        }
        try (ResultSet resultSet =
            reader.query(schema.getTableName(), columnNames, Long.MIN_VALUE, Long.MAX_VALUE)) {
          ResultSetMetadata metadata = resultSet.getMetadata();
          while (resultSet.next()) {
            long time = resultSet.getLong(1);
            table.rows++;
            if (table.minTime == null || time < Times.parseToNanos(table.minTime)) {
              table.minTime = Times.formatExact(time);
            }
            if (table.maxTime == null || time > Times.parseToNanos(table.maxTime)) {
              table.maxTime = Times.formatExact(time);
            }
            if (!tagIndexes.isEmpty()) {
              StringBuilder key = new StringBuilder();
              for (Integer index : tagIndexes) {
                key.append(resultSet.isNull(index + 2) ? "" : resultSet.getString(index + 2)).append('\u0001');
              }
              devices.add(key.toString());
            }
            if (table.sample.size() < sampleRows) {
              table.sample.add(formatRow(resultSet, columnNames));
            }
          }
        } catch (Exception e) {
          LOG.warn("cannot read data of table {} in {}: {}", table.table, inspection.file, e.getMessage());
        }
        table.devices = tagIndexes.isEmpty() ? (table.rows > 0 ? 1 : 0) : devices.size();
        inspection.rows += table.rows;
        inspection.devices += table.devices;
        if (table.minTime != null
            && (inspection.minTime == null
                || Times.parseToNanos(table.minTime) < Times.parseToNanos(inspection.minTime))) {
          inspection.minTime = table.minTime;
        }
        if (table.maxTime != null
            && (inspection.maxTime == null
                || Times.parseToNanos(table.maxTime) > Times.parseToNanos(inspection.maxTime))) {
          inspection.maxTime = table.maxTime;
        }
        inspection.tables.add(table);
      }
    }
    return inspection;
  }

  private static String formatRow(ResultSet resultSet, List<String> columnNames) throws IOException {
    ResultSetMetadata metadata = resultSet.getMetadata();
    StringBuilder sb = new StringBuilder();
    sb.append("time=").append(Times.formatExact(resultSet.getLong(1)));
    for (int i = 0; i < columnNames.size(); i++) {
      int index = i + 2;
      sb.append(", ").append(columnNames.get(i)).append('=');
      if (resultSet.isNull(index)) {
        sb.append("null");
        continue;
      }
      TSDataType type = metadata.getColumnType(index);
      switch (type) {
        case BOOLEAN:
          sb.append(resultSet.getBoolean(index));
          break;
        case INT32:
        case INT64:
          sb.append(resultSet.getLong(index));
          break;
        case FLOAT:
        case DOUBLE:
          sb.append(resultSet.getDouble(index));
          break;
        default:
          sb.append(resultSet.getString(index));
          break;
      }
    }
    return sb.toString();
  }
}
