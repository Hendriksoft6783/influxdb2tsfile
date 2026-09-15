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
package com.timecho.influxdb2tsfile.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.SchemaOptions;
import com.timecho.influxdb2tsfile.core.TableSpec;
import com.timecho.influxdb2tsfile.pipeline.ManifestSupport;
import com.timecho.influxdb2tsfile.sink.ManifestStore;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;
import com.timecho.influxdb2tsfile.source.influx.InfluxSource;
import com.timecho.influxdb2tsfile.source.influx.ServerInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(
    name = "discover",
    mixinStandardHelpOptions = true,
    description = "Connect to InfluxDB and print the measurements, tags, fields and the TsFile mapping")
public class DiscoverCommand implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(DiscoverCommand.class);

  @Option(names = "--measurements", split = ",", description = "only these measurements (default: all)")
  public List<String> measurements;

  @Option(names = "--measurement-regex", description = "only measurements matching this regular expression")
  public String measurementRegex;

  @Option(names = "--ignore-case", description = "ignore the case of measurement names")
  public boolean ignoreCase;

  @Option(names = "--counts", description = "estimate the point count of every measurement by counting its first field (approximate, can be slow)")
  public boolean counts;

  @Option(names = "--json", description = "print the result as JSON")
  public boolean json;

  @Option(names = "--report", description = "also write the JSON report to this file")
  public String report;

  @Mixin public InfluxOptionsMixin influx;

  @Mixin public WriterOptionsMixin writer;

  @Mixin public CommonOptions common;

  @Override
  public Integer call() {
    try (InfluxSource source =
        new InfluxSource(
            influx.toClient(),
            influx.database,
            influx.retentionPolicy,
            influx.chunkSize <= 0 ? 10000 : influx.chunkSize)) {
      ServerInfo info = source.ping();
      if (!json) {
        LOG.info("connected to InfluxDB {} at {}", info, influx.url);
      }
      String retentionPolicy =
          influx.retentionPolicy != null && !influx.retentionPolicy.isEmpty()
              ? influx.retentionPolicy
              : source.defaultRetentionPolicy();
      source.setRetentionPolicy(retentionPolicy);
      List<String> available = source.showMeasurements();
      List<String> selected = MeasurementFilter.select(available, measurements, measurementRegex, ignoreCase);
      if (selected.isEmpty()) {
        throw new MigrationException(
            "no measurement matched (available measurements: " + available.size() + ")");
      }
      Map<String, MeasurementSchema> schemas = source.describe(selected);
      SchemaOptions schemaOptions = writer.toSchemaOptions();
      List<String> skipped = new ArrayList<>();
      List<TableSpec> specs = ManifestSupport.planSpecs(schemas, schemaOptions, skipped, LOG);

      Map<String, Long> pointCounts = new LinkedHashMap<>();
      Map<String, Object> reportModel = new LinkedHashMap<>();
      reportModel.put("url", influx.url);
      reportModel.put("serverVersion", info.version);
      reportModel.put("database", influx.database);
      reportModel.put("retentionPolicy", retentionPolicy);
      List<Map<String, Object>> measurementModels = new ArrayList<>();
      for (TableSpec spec : specs) {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("measurement", spec.measurement());
        model.put("table", spec.tableName());
        model.put("tags", spec.tagKeys());
        model.put("tagColumns", spec.tagColumns());
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i < spec.fieldKeys().size(); i++) {
          fields.put(spec.fieldKeys().get(i), spec.fieldTypes().get(i).name());
        }
        model.put("fields", fields);
        if (counts) {
          Long points = source.countPoints(spec.measurement(), spec.fieldKeys().get(0));
          model.put("points", points);
          if (points != null) {
            pointCounts.put(spec.measurement(), points);
          }
        }
        measurementModels.add(model);
      }
      reportModel.put("measurements", measurementModels);
      reportModel.put("skipped", skipped);
      reportModel.put("availableMeasurements", available.size());

      if (json) {
        System.out.println(ManifestStore.toJson(reportModel));
      } else {
        printText(info, retentionPolicy, available, specs, skipped, pointCounts);
      }
      if (report != null && !report.isEmpty()) {
        Path path = Paths.get(report);
        if (path.getParent() != null) {
          Files.createDirectories(path.getParent());
        }
        Files.write(path, ManifestStore.toJson(reportModel).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        LOG.info("report written to {}", path);
      }
      return 0;
    } catch (java.io.IOException e) {
      throw new MigrationException("discover failed: " + e.getMessage(), e);
    }
  }

  private void printText(
      ServerInfo info,
      String retentionPolicy,
      List<String> available,
      List<TableSpec> specs,
      List<String> skipped,
      Map<String, Long> pointCounts) {
    System.out.println("server: " + info + " at " + influx.url);
    System.out.println("database: " + influx.database + ", retention policy: " + retentionPolicy);
    System.out.println("measurements: " + available.size() + " in total, " + specs.size() + " to migrate");
    for (TableSpec spec : specs) {
      StringBuilder fields = new StringBuilder();
      for (int i = 0; i < spec.fieldKeys().size(); i++) {
        if (fields.length() > 0) {
          fields.append(", ");
        }
        fields.append(spec.fieldKeys().get(i)).append('(').append(spec.fieldTypes().get(i)).append(')');
      }
      Long points = pointCounts.get(spec.measurement());
      System.out.println(
          "  "
              + spec.measurement()
              + " -> table "
              + spec.tableName()
              + (points == null ? "" : ", points=" + points));
      System.out.println("    tags:   " + (spec.tagKeys().isEmpty() ? "-" : String.join(", ", spec.tagKeys())));
      System.out.println("    fields: " + fields);
    }
    if (!skipped.isEmpty()) {
      System.out.println("skipped: " + String.join(", ", skipped));
    }
  }
}