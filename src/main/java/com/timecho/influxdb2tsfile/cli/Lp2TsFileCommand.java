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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.Naming;
import com.timecho.influxdb2tsfile.core.Progress;
import com.timecho.influxdb2tsfile.core.SchemaOptions;
import com.timecho.influxdb2tsfile.core.SizeFormat;
import com.timecho.influxdb2tsfile.core.TableSpec;
import com.timecho.influxdb2tsfile.core.TimePrecision;
import com.timecho.influxdb2tsfile.core.Times;
import com.timecho.influxdb2tsfile.core.WriteOptions;
import com.timecho.influxdb2tsfile.pipeline.ManifestSupport;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.ManifestStore;
import com.timecho.influxdb2tsfile.sink.UnitResult;
import com.timecho.influxdb2tsfile.sink.UnitWriter;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;
import com.timecho.influxdb2tsfile.source.lp.LineProtocolParser;
import com.timecho.influxdb2tsfile.source.lp.LineProtocolSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "lp2tsfile",
    mixinStandardHelpOptions = true,
    description = "Convert line protocol files (or stdin) into TsFile")
public class Lp2TsFileCommand implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(Lp2TsFileCommand.class);

  @Parameters(arity = "1..*", paramLabel = "<file>", description = "line protocol files or directories, - reads from stdin")
  public List<String> inputs;

  @Option(names = "--precision", description = "timestamp precision of the input: s, ms, us or ns (default: ${DEFAULT-VALUE})", defaultValue = "ns")
  public String precision;

  @Option(names = "--default-time", description = "timestamp used for points without timestamp, e.g. now or 2024-01-01T00:00:00Z")
  public String defaultTime;

  @Option(names = "--dry-run", description = "only print the plan, do not write any file")
  public boolean dryRun;

  @Option(names = "--skip-invalid", description = "skip invalid line protocol lines instead of failing")
  public boolean skipInvalid;

  @Option(names = "--manifest", description = "manifest file (default: <out>/manifest.json)")
  public String manifestPath;

  @Mixin public WriterOptionsMixin writer;

  @Mixin public CommonOptions common;

  @Override
  public Integer call() throws Exception {
    List<Path> files = new ArrayList<>();
    Path spooled = null;
    boolean fromStdin = false;
    for (String input : inputs) {
      if ("-".equals(input)) {
        fromStdin = true;
        spooled = Files.createTempFile("influxdb2tsfile-lp", ".lp");
        try (InputStream in = System.in) {
          Files.copy(in, spooled, StandardCopyOption.REPLACE_EXISTING);
        }
        files.add(spooled);
        continue;
      }
      Path path = Paths.get(input);
      if (!Files.exists(path)) {
        throw new MigrationException("input file does not exist: " + input);
      }
      if (Files.isDirectory(path)) {
        try (java.util.stream.Stream<Path> stream = Files.list(path)) {
          stream
              .filter(Files::isRegularFile)
              .filter(Lp2TsFileCommand::looksLikeLineProtocol)
              .sorted()
              .forEach(files::add);
        }
      } else {
        files.add(path);
      }
    }
    if (files.isEmpty()) {
      throw new MigrationException(
          "no line protocol file found (a directory is scanned for *.lp, *.txt and *.lineprotocol files)");
    }
    TimePrecision timePrecision = TimePrecision.valueOf(precision.trim().toUpperCase());
    Long defaultTimeNanos = defaultTime == null || defaultTime.isEmpty() ? null : Times.parseToNanos(defaultTime);
    LineProtocolParser parser = new LineProtocolParser(timePrecision, defaultTimeNanos);
    LineProtocolSource source = new LineProtocolSource(files, parser);

    LOG.info("scanning {} file(s) to build the schema", files.size());
    List<MeasurementSchema> schemas = source.discover();
    SchemaOptions schemaOptions = writer.toSchemaOptions();
    List<String> skipped = new ArrayList<>();
    Map<String, MeasurementSchema> schemaMap = new LinkedHashMap<>();
    for (MeasurementSchema schema : schemas) {
      schemaMap.put(schema.measurement(), schema);
    }
    List<TableSpec> specs = ManifestSupport.planSpecs(schemaMap, schemaOptions, skipped, LOG);
    for (TableSpec spec : specs) {
      LOG.info(
          "table {}: {} tags [{}], {} fields [{}]",
          spec.tableName(),
          spec.tagCount(),
          String.join(",", spec.tagColumns()),
          spec.fieldCount(),
          String.join(",", spec.fieldColumns()));
    }
    if (!skipped.isEmpty()) {
      LOG.warn("{} measurement(s) skipped: {}", skipped.size(), String.join(",", skipped));
    }
    if (specs.isEmpty()) {
      throw new MigrationException("no migratable measurement found");
    }
    if (dryRun) {
      LOG.info(
          "dry run: {} point(s) in {} file(s) would be converted into {} table(s)",
          source.pointCount(),
          files.size(),
          specs.size());
      return 0;
    }

    WriteOptions writeOptions = writer.toWriteOptions();
    Files.createDirectories(writeOptions.outDir);
    String unitId = fromStdin ? "stdin" : unitId(files, writeOptions);
    Progress progress = new Progress();
    Manifest manifest = ManifestSupport.newManifest();
    manifest.sourceType = "line-protocol";
    manifest.measurements.addAll(schemaMap.keySet());
    ManifestSupport.fillTableRecords(manifest, specs);
    manifest.options.put("input", String.join(",", inputs));
    manifest.options.put("precision", precision);
    manifest.options.put("out", writeOptions.outDir.toString());

    LOG.info("writing table(s) into {}/{}", writeOptions.outDir, writeOptions.partFileName(unitId, 0));
    source.setSkipInvalid(skipInvalid);
    UnitResult result;
    UnitWriter unitWriter =
        new UnitWriter(writeOptions.outDir, writeOptions, unitId, specs, progress);
    try {
      source.scanAll(
          (measurement, tags, timeNanos, fields) ->
              unitWriter.addRow(measurement, tags, timeNanos, fields));
    } finally {
      unitWriter.close();
    }
    result = unitWriter.result();
    if (source.invalidLines() > 0) {
      LOG.warn("{} invalid line(s) were skipped, for example: {}", source.invalidLines(), source.invalidSamples());
    }
    manifest.units.add(result);
    manifest.recomputeTotals();
    Path manifestFile =
        manifestPath == null || manifestPath.isEmpty()
            ? writeOptions.outDir.resolve(
                writeOptions.filePrefix == null || writeOptions.filePrefix.isEmpty()
                    ? "manifest.json"
                    : writeOptions.filePrefix + "-manifest.json")
            : Paths.get(manifestPath);
    ManifestStore.save(manifestFile, manifest);
    LOG.info(
        "done: {} rows, {} file(s), {} written into {}; manifest: {}",
        result.rows,
        result.files.size(),
        SizeFormat.format(result.bytes()),
        writeOptions.outDir,
        manifestFile);
    LOG.info(progress.summary());
    if (spooled != null) {
      Files.deleteIfExists(spooled);
    }
    return 0;
  }

  private static boolean looksLikeLineProtocol(Path path) {
    String name = path.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
    if (name.startsWith(".")) {
      return false;
    }
    return name.endsWith(".lp") || name.endsWith(".txt") || name.endsWith(".lineprotocol");
  }

  private static String unitId(List<Path> files, WriteOptions writeOptions) {
    String base =
        writeOptions.filePrefix == null || writeOptions.filePrefix.isEmpty()
            ? files.get(0).getFileName().toString().replaceAll("\\.[A-Za-z0-9]+$", "")
            : writeOptions.filePrefix;
    String sanitized = Naming.sanitize(base);
    return sanitized.isEmpty() ? "data" : sanitized;
  }
}