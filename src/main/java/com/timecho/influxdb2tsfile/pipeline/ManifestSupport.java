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
package com.timecho.influxdb2tsfile.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.timecho.influxdb2tsfile.Version;
import com.timecho.influxdb2tsfile.core.SchemaConflictException;
import com.timecho.influxdb2tsfile.core.SchemaOptions;
import com.timecho.influxdb2tsfile.core.SchemaPlanner;
import com.timecho.influxdb2tsfile.core.TableSpec;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;

import org.slf4j.Logger;

public final class ManifestSupport {

  private ManifestSupport() {}

  public static List<TableSpec> planSpecs(
      Map<String, MeasurementSchema> schemas, SchemaOptions options, List<String> skipped, Logger log) {
    List<TableSpec> specs = new ArrayList<>();
    for (MeasurementSchema schema : schemas.values()) {
      try {
        specs.add(SchemaPlanner.plan(schema, options));
      } catch (SchemaConflictException e) {
        log.error("skip measurement '{}': {}", schema.measurement(), e.getMessage());
        skipped.add(schema.measurement());
      }
    }
    return specs;
  }

  public static void fillTableRecords(Manifest manifest, List<TableSpec> specs) {
    for (TableSpec spec : specs) {
      Manifest.TableRecord record = new Manifest.TableRecord();
      record.table = spec.tableName();
      record.measurement = spec.measurement();
      for (int i = 0; i < spec.tagColumns().size(); i++) {
        record.columns.add(
            column(spec.tagColumns().get(i), spec.tagKeys().get(i), "STRING", "TAG"));
      }
      for (int i = 0; i < spec.fieldColumns().size(); i++) {
        record.columns.add(
            column(
                spec.fieldColumns().get(i),
                spec.fieldKeys().get(i),
                spec.fieldTypes().get(i).name(),
                "FIELD"));
      }
      manifest.tables.add(record);
    }
  }

  private static Manifest.ColumnRecord column(String name, String source, String type, String category) {
    Manifest.ColumnRecord record = new Manifest.ColumnRecord();
    record.column = name;
    record.source = source;
    record.type = type;
    record.category = category;
    return record;
  }

  public static Manifest newManifest() {
    Manifest manifest = new Manifest();
    manifest.tool = Version.TOOL_NAME;
    manifest.version = Version.VERSION;
    manifest.tsfileVersion = Version.TSFILE_VERSION;
    manifest.createdAt = java.time.Instant.now().toString();
    return manifest;
  }

  public static Map<String, Long> rowsByTable(Manifest manifest) {
    Map<String, Long> rows = new TreeMap<>();
    for (com.timecho.influxdb2tsfile.sink.UnitResult unit : manifest.units) {
      if (!"done".equals(unit.status)) {
        continue;
      }
      for (Map.Entry<String, com.timecho.influxdb2tsfile.sink.TableStat> entry : unit.tables.entrySet()) {
        rows.merge(entry.getKey(), entry.getValue().rows, Long::sum);
      }
    }
    return rows;
  }
}
