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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.timecho.influxdb2tsfile.source.MeasurementSchema;

public final class SchemaPlanner {

  private SchemaPlanner() {}

  public static TableSpec plan(MeasurementSchema schema, SchemaOptions options) {
    String baseName = options.sanitizeNames ? Naming.sanitize(schema.measurement()) : lower(schema.measurement());
    String tableName =
        options.tablePrefix == null || options.tablePrefix.isEmpty()
            ? baseName
            : options.tablePrefix + "_" + baseName;

    Set<String> used = new HashSet<>();
    List<String> tagKeys = new ArrayList<>();
    List<String> tagColumns = new ArrayList<>();
    List<String> fieldKeys = new ArrayList<>();
    List<String> fieldColumns = new ArrayList<>();
    List<org.apache.tsfile.enums.TSDataType> fieldTypes = new ArrayList<>();

    if (options.includeTags) {
      for (String tagKey : schema.tagKeys()) {
        if (options.includeTagKeys != null && !options.includeTagKeys.contains(tagKey)) {
          continue;
        }
        if (options.excludeTagKeys.contains(tagKey)) {
          continue;
        }
        String column = options.sanitizeNames ? Naming.sanitize(tagKey) : lower(tagKey);
        column = Naming.unique(column, used);
        tagKeys.add(tagKey);
        tagColumns.add(column);
      }
    }

    for (Map.Entry<String, InfluxType> entry : schema.fields().entrySet()) {
      String fieldKey = entry.getKey();
      if (options.excludeFieldKeys.contains(fieldKey)) {
        continue;
      }
      InfluxType type = options.fieldTypeOverrides.get(fieldKey.toLowerCase(Locale.ROOT));
      if (type == null) {
        type = options.fieldTypeOverrides.get(fieldKey);
      }
      if (type == null) {
        type = entry.getValue();
      }
      if (schema.conflicts().contains(fieldKey)) {
        if (options.typeConflictPolicy == SchemaOptions.TypeConflictPolicy.FAIL) {
          throw new SchemaConflictException(
              "field '"
                  + fieldKey
                  + "' of measurement '"
                  + schema.measurement()
                  + "' has more than one data type in InfluxDB; "
                  + "use --type-conflict text to store it as string, or query it slice by slice with --time-slice");
        }
        type = InfluxType.STRING;
      }
      String column = options.sanitizeNames ? Naming.sanitize(fieldKey) : lower(fieldKey);
      column = Naming.unique(column, used);
      fieldKeys.add(fieldKey);
      fieldColumns.add(column);
      fieldTypes.add(type.tsDataType());
    }

    if (fieldKeys.isEmpty()) {
      throw new SchemaConflictException(
          "measurement '" + schema.measurement() + "' has no field to migrate");
    }
    return new TableSpec(
        schema.measurement(), tableName, tagKeys, tagColumns, fieldKeys, fieldColumns, fieldTypes);
  }

  private static String lower(String value) {
    return value.toLowerCase(Locale.ROOT);
  }
}
