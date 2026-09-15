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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.SizeFormat;
import com.timecho.influxdb2tsfile.inspect.FileInspection;
import com.timecho.influxdb2tsfile.inspect.TsFileInspector;
import com.timecho.influxdb2tsfile.pipeline.Verifier;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.ManifestStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "inspect",
    mixinStandardHelpOptions = true,
    description = "Show the structure and the row count of generated TsFile files")
public class InspectCommand implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(InspectCommand.class);

  @Parameters(arity = "1..*", paramLabel = "<path>", description = "tsfile files or directories")
  public List<String> paths;

  @Option(names = "--rows", description = "print the first N rows of every table (default: ${DEFAULT-VALUE})", defaultValue = "0")
  public int rows;

  @Option(names = "--json", description = "print the result as JSON")
  public boolean json;

  @Option(names = "--manifest", description = "compare the files against a manifest written by migrate or lp2tsfile")
  public String manifest;

  @Mixin public CommonOptions common;

  @Override
  public Integer call() throws Exception {
    List<Path> files = new ArrayList<>();
    for (String path : paths) {
      Path p = Paths.get(path);
      if (!Files.exists(p)) {
        throw new MigrationException("path does not exist: " + path);
      }
      if (Files.isDirectory(p)) {
        try (Stream<Path> stream = Files.list(p)) {
          stream
              .filter(Files::isRegularFile)
              .filter(f -> f.getFileName().toString().endsWith(".tsfile"))
              .sorted()
              .forEach(files::add);
        }
      } else {
        files.add(p);
      }
    }
    if (files.isEmpty()) {
      throw new MigrationException("no tsfile found");
    }
    List<FileInspection> inspections = new ArrayList<>();
    long totalRows = 0;
    long totalBytes = 0;
    for (Path file : files) {
      FileInspection inspection = TsFileInspector.inspect(file, rows);
      inspections.add(inspection);
      totalRows += inspection.rows;
      totalBytes += inspection.bytes;
    }
    if (json) {
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("files", inspections);
      result.put("totalRows", totalRows);
      result.put("totalBytes", totalBytes);
      System.out.println(ManifestStore.toJson(result));
    } else {
      printText(inspections, totalRows, totalBytes);
    }
    if (manifest != null && !manifest.isEmpty()) {
      Path manifestFile = Paths.get(manifest);
      if (!Files.exists(manifestFile)) {
        throw new MigrationException("manifest does not exist: " + manifest);
      }
      Manifest loaded = ManifestStore.load(manifestFile);
      Path outDir = manifestFile.toAbsolutePath().getParent();
      boolean ok = Verifier.verify(loaded, outDir);
      return ok ? 0 : 1;
    }
    return 0;
  }

  private void printText(List<FileInspection> inspections, long totalRows, long totalBytes) {
    for (FileInspection inspection : inspections) {
      System.out.println("file: " + inspection.file + "  (" + SizeFormat.format(inspection.bytes) + ")");
      System.out.println(
          "  rows="
              + inspection.rows
              + ", devices="
              + inspection.devices
              + ", range=["
              + nullToDash(inspection.minTime)
              + ", "
              + nullToDash(inspection.maxTime)
              + "]");
      for (FileInspection.TableInspection table : inspection.tables) {
        System.out.println(
            "  table " + table.table + ": rows=" + table.rows + ", devices=" + table.devices + ", range=["
                + nullToDash(table.minTime) + ", " + nullToDash(table.maxTime) + "]");
        StringBuilder columns = new StringBuilder();
        for (FileInspection.ColumnInspection column : table.columns) {
          if (columns.length() > 0) {
            columns.append(", ");
          }
          columns.append(column.name).append('(').append(column.category).append('/').append(column.type).append(')');
        }
        System.out.println("    columns: " + columns);
        for (String row : table.sample) {
          System.out.println("    " + row);
        }
      }
    }
    System.out.println(
        "total: " + inspections.size() + " file(s), " + totalRows + " rows, " + SizeFormat.format(totalBytes));
  }

  private static String nullToDash(String value) {
    return value == null ? "-" : value;
  }
}