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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.timecho.influxdb2tsfile.core.DurationFormat;
import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.Naming;
import com.timecho.influxdb2tsfile.core.Progress;
import com.timecho.influxdb2tsfile.core.SchemaOptions;
import com.timecho.influxdb2tsfile.core.SizeFormat;
import com.timecho.influxdb2tsfile.core.TableSpec;
import com.timecho.influxdb2tsfile.core.Times;
import com.timecho.influxdb2tsfile.core.WriteOptions;
import com.timecho.influxdb2tsfile.pipeline.ManifestSupport;
import com.timecho.influxdb2tsfile.pipeline.MigrationUnit;
import com.timecho.influxdb2tsfile.pipeline.Verifier;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.ManifestStore;
import com.timecho.influxdb2tsfile.sink.TableStat;
import com.timecho.influxdb2tsfile.sink.UnitResult;
import com.timecho.influxdb2tsfile.sink.UnitWriter;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;
import com.timecho.influxdb2tsfile.source.influx.InfluxSource;
import com.timecho.influxdb2tsfile.source.influx.ServerInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(
    name = "migrate",
    mixinStandardHelpOptions = true,
    description = "Read data from InfluxDB and write TsFile files")
public class MigrateCommand implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateCommand.class);

  @Option(names = "--measurements", split = ",", description = "measurements to migrate (default: all)")
  public List<String> measurements;

  @Option(names = "--measurement-regex", description = "only measurements matching this regular expression")
  public String measurementRegex;

  @Option(names = "--ignore-case", description = "ignore the case of measurement names")
  public boolean ignoreCase;

  @Option(names = "--start", description = "start time, RFC3339, epoch or relative like -30d (default: ${DEFAULT-VALUE})", defaultValue = "1970-01-01T00:00:00Z")
  public String start;

  @Option(names = "--end", description = "end time, exclusive, RFC3339, epoch or now (default: ${DEFAULT-VALUE})", defaultValue = "now")
  public String end;

  @Option(names = "--time-slice", description = "split every measurement into slices, e.g. 7d or 1h; improves memory usage and resuming")
  public String timeSlice;

  @Option(names = "--parallel", description = "number of units migrated in parallel (default: ${DEFAULT-VALUE})", defaultValue = "1")
  public int parallel;

  @Option(names = "--retries", description = "retries of a failed unit (default: ${DEFAULT-VALUE})", defaultValue = "1")
  public int retries;

  @Option(names = "--resume", description = "reuse the manifest of a previous run and skip the completed units")
  public boolean resume;

  @Option(names = "--dry-run", description = "print the migration plan without reading any data")
  public boolean dryRun;

  @Option(names = "--verify", description = "read every generated tsfile part back and compare the row counts")
  public boolean verify;

  @Option(names = "--fail-fast", description = "stop at the first failed unit")
  public boolean failFast;

  @Option(names = "--manifest", description = "manifest file (default: <out>/manifest.json)")
  public String manifestPath;

  @Mixin public InfluxOptionsMixin influx;

  @Mixin public WriterOptionsMixin writer;

  @Mixin public CommonOptions common;

  @Override
  public Integer call() {
    long startNanos = Times.parseToNanos(start);
    long endNanos = Times.parseToNanos(end);
    if (endNanos <= startNanos) {
      throw new MigrationException("--end must be greater than --start");
    }
    Long sliceNanos =
        timeSlice == null || timeSlice.isEmpty() ? null : DurationFormat.parse(timeSlice).toNanos();
    WriteOptions writeOptions = writer.toWriteOptions();
    Path manifestFile =
        manifestPath == null || manifestPath.isEmpty()
            ? writeOptions.outDir.resolve(
                writeOptions.filePrefix == null || writeOptions.filePrefix.isEmpty()
                    ? "manifest.json"
                    : writeOptions.filePrefix + "-manifest.json")
            : Paths.get(manifestPath);

    try (InfluxSource source =
        new InfluxSource(
            influx.toClient(),
            influx.database,
            influx.retentionPolicy,
            influx.chunkSize <= 0 ? 10000 : influx.chunkSize)) {
      ServerInfo info = source.ping();
      LOG.info("connected to InfluxDB {} at {}", info, influx.url);
      checkServerVersion(info);
      String retentionPolicy =
          influx.retentionPolicy != null && !influx.retentionPolicy.isEmpty()
              ? influx.retentionPolicy
              : source.defaultRetentionPolicy();
      source.setRetentionPolicy(retentionPolicy);
      List<String> available = source.showMeasurements();
      List<String> selected = MeasurementFilter.select(available, measurements, measurementRegex, ignoreCase);
      if (selected.isEmpty()) {
        throw new MigrationException("no measurement matched, available measurements: " + available.size());
      }
      LOG.info(
          "from {} to {}: {} of {} measurement(s) are selected",
          Times.format(startNanos),
          Times.format(endNanos),
          selected.size(),
          available.size());
      Map<String, MeasurementSchema> schemas = source.describe(selected);
      SchemaOptions schemaOptions = writer.toSchemaOptions();
      List<String> skipped = new ArrayList<>();
      List<TableSpec> specs = ManifestSupport.planSpecs(schemas, schemaOptions, skipped, LOG);
      if (specs.isEmpty()) {
        throw new MigrationException("no migratable measurement found");
      }
      Map<String, TableSpec> specByMeasurement = new LinkedHashMap<>();
      for (TableSpec spec : specs) {
        specByMeasurement.put(spec.measurement(), spec);
      }
      List<MigrationUnit> units = buildUnits(specs, startNanos, endNanos, sliceNanos, source);
      printPlan(specs, units, retentionPolicy, skipped);
      if (dryRun) {
        LOG.info("dry run: nothing was read or written");
        return 0;
      }

      Files.createDirectories(writeOptions.outDir);
      Manifest manifest = loadOrCreateManifest(manifestFile);
      fillManifest(manifest, info, retentionPolicy, selected, specs, writeOptions);
      Set<String> done = new HashSet<>();
      int invalidated = 0;
      for (UnitResult unit : manifest.units) {
        if (!"done".equals(unit.status)) {
          continue;
        }
        if (unitFilesValid(unit, writeOptions.outDir)) {
          done.add(unit.unitId);
        } else {
          invalidated++;
        }
      }
      if (invalidated > 0) {
        LOG.warn(
            "{} completed unit(s) in {} have missing or changed output files, they are migrated again",
            invalidated,
            manifestFile.getFileName());
      }
      List<MigrationUnit> pending = new ArrayList<>();
      for (MigrationUnit unit : units) {
        if (done.contains(unit.id)) {
          continue;
        }
        pending.add(unit);
      }
      if (!done.isEmpty()) {
        LOG.info("{} unit(s) already completed in {} are skipped", done.size(), manifestFile.getFileName());
      }
      if (pending.isEmpty()) {
        LOG.info("nothing to do, all {} unit(s) are completed", units.size());
      }
      Progress progress = new Progress();
      int failures = runUnits(manifest, manifestFile, pending, specByMeasurement, source, writeOptions, progress);
      manifest.recomputeTotals();
      ManifestStore.save(manifestFile, manifest);
      printSummary(manifest, writeOptions, manifestFile, skipped);
      if (verify) {
        boolean ok = Verifier.verify(manifest, writeOptions.outDir);
        if (!ok) {
          return 1;
        }
      }
      if (failures > 0) {
        LOG.error("{} unit(s) failed, see the manifest for details: {}", failures, manifestFile);
      }
      return failures == 0 ? 0 : 1;
    } catch (java.io.IOException e) {
      throw new MigrationException("migration failed: " + e.getMessage(), e);
    }
  }

  private void checkServerVersion(ServerInfo info) {
    if (influx.serverVersion == null || influx.serverVersion.equalsIgnoreCase("auto")) {
      if (info.majorVersion == 2) {
        LOG.info(
            "server is InfluxDB 2.x; it is queried through the InfluxQL compatibility API. "
                + "If a query fails with 'database not found', create the DBRP mapping with: "
                + "influx v1 dbrp create --db {} --rp autogen --bucket-id <bucket-id> --default",
            influx.database);
      }
      return;
    }
    if (info.majorVersion != 0) {
      int expected = Integer.parseInt(influx.serverVersion.trim());
      if (expected != info.majorVersion) {
        LOG.warn("the server reports version {} but --server-version {} was given", info.version, influx.serverVersion);
      }
    }
  }

  private Manifest loadOrCreateManifest(Path manifestFile) {
    if (resume && Files.exists(manifestFile)) {
      try {
        Manifest manifest = ManifestStore.load(manifestFile);
        LOG.info("resuming from {}", manifestFile);
        return manifest;
      } catch (java.io.IOException e) {
        throw new MigrationException("cannot read manifest " + manifestFile + ": " + e.getMessage(), e);
      }
    }
    return ManifestSupport.newManifest();
  }

  private void fillManifest(
      Manifest manifest,
      ServerInfo info,
      String retentionPolicy,
      List<String> selected,
      List<TableSpec> specs,
      WriteOptions writeOptions) {
    manifest.sourceType = info.majorVersion == 2 ? "influxdb2" : "influxdb1";
    manifest.url = influx.url;
    manifest.database = influx.database;
    if (manifest.measurements.isEmpty()) {
      manifest.measurements.addAll(selected);
    }
    if (manifest.tables.isEmpty()) {
      ManifestSupport.fillTableRecords(manifest, specs);
    }
    Map<String, String> options = new LinkedHashMap<>();
    options.put("url", String.valueOf(influx.url));
    options.put("database", String.valueOf(influx.database));
    options.put("retentionPolicy", String.valueOf(retentionPolicy));
    options.put("start", start);
    options.put("end", end);
    options.put("timeSlice", String.valueOf(timeSlice));
    options.put("parallel", String.valueOf(parallel));
    options.put("out", writeOptions.outDir.toString());
    options.put("filePrefix", String.valueOf(writeOptions.filePrefix));
    options.put("tablePrefix", String.valueOf(writeOptions.tablePrefix));
    options.put("maxFileSizeBytes", String.valueOf(writeOptions.maxFileSizeBytes));
    options.put("batchSize", String.valueOf(writeOptions.batchSize));
    options.put("compression", String.valueOf(writeOptions.compression));
    options.put("sanitizeNames", String.valueOf(writeOptions.sanitizeNames));
    manifest.options = options;
  }

  private int runUnits(
      Manifest manifest,
      Path manifestFile,
      List<MigrationUnit> pending,
      Map<String, TableSpec> specByMeasurement,
      InfluxSource source,
      WriteOptions writeOptions,
      Progress progress) {
    if (pending.isEmpty()) {
      return 0;
    }
    int parallelism = Math.max(1, Math.min(parallel, pending.size()));
    ExecutorService pool = Executors.newFixedThreadPool(parallelism);
    List<Future<UnitResult>> futures = new ArrayList<>();
    for (MigrationUnit unit : pending) {
      futures.add(
          pool.submit(
              () ->
                  runUnitWithRetries(
                      unit,
                      specByMeasurement.get(unit.measurement),
                      source,
                      writeOptions,
                      progress)));
    }
    pool.shutdown();
    int failures = 0;
    long[] lastSave = {System.currentTimeMillis()};
    for (Future<UnitResult> future : futures) {
      UnitResult result;
      try {
        result = future.get();
      } catch (ExecutionException e) {
        throw new MigrationException("unit execution failed: " + e.getCause(), e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MigrationException("migration interrupted", e);
      }
      if ("done".equals(result.status) && result.rows == 0) {
        result.status = "empty";
      }
      synchronized (manifest) {
        manifest.units.removeIf(
            unit -> unit.unitId != null && unit.unitId.equals(result.unitId));
        manifest.units.add(result);
        manifest.recomputeTotals();
        long now = System.currentTimeMillis();
        if (now - lastSave[0] > 2000 || !"done".equals(result.status)) {
          lastSave[0] = now;
          try {
            ManifestStore.save(manifestFile, manifest);
          } catch (java.io.IOException e) {
            throw new MigrationException("cannot save manifest " + manifestFile + ": " + e.getMessage(), e);
          }
        }
      }
      if ("done".equals(result.status)) {
        LOG.info(
            "unit {} finished: {} rows, {} device(s), {} file(s), {}",
            result.unitId,
            result.rows,
            result.devices(),
            result.files.size(),
            SizeFormat.format(result.bytes()));
      } else if ("empty".equals(result.status)) {
        LOG.debug("unit {} contains no row in the selected time range", result.unitId);
      } else {
        failures++;
        LOG.error("unit {} failed: {}", result.unitId, result.error);
        if (failFast) {
          break;
        }
      }
    }
    return failures;
  }

  private UnitResult runUnitWithRetries(
      MigrationUnit unit,
      TableSpec spec,
      InfluxSource source,
      WriteOptions writeOptions,
      Progress progress) {
    RuntimeException last = null;
    int attempts = Math.max(1, retries + 1);
    for (int attempt = 1; attempt <= attempts; attempt++) {
      deleteUnitFiles(writeOptions, unit.id);
      UnitWriter unitWriter =
          new UnitWriter(
              writeOptions.outDir, writeOptions, unit.id, Collections.singletonList(spec), progress);
      try {
        source.query(
            unit.measurement,
            unit.startNanos,
            unit.endNanos,
            (measurement, tags, timeNanos, fields) ->
                unitWriter.addRow(measurement, tags, timeNanos, fields));
      } catch (RuntimeException e) {
        try {
          unitWriter.close();
        } catch (RuntimeException closeError) {
          LOG.debug("cannot close writer of unit {}", unit.id, closeError);
        }
        last = e;
        LOG.warn("unit {} attempt {}/{} failed: {}", unit.id, attempt, attempts, e.getMessage());
        continue;
      }
      unitWriter.close();
      try {
        return unitWriter.result();
      } catch (RuntimeException e) {
        last = e;
        LOG.warn("unit {} attempt {}/{} failed: {}", unit.id, attempt, attempts, e.getMessage());
      }
    }
    UnitResult failed = new UnitResult();
    failed.unitId = unit.id;
    failed.status = "failed";
    failed.error = last == null ? "unknown error" : last.getMessage();
    return failed;
  }

  private boolean unitFilesValid(UnitResult unit, Path outDir) {
    if (unit.files == null || unit.files.isEmpty()) {
      return unit.rows == 0;
    }
    for (com.timecho.influxdb2tsfile.sink.UnitFile file : unit.files) {
      Path path = outDir.resolve(file.name);
      if (!Files.exists(path)) {
        return false;
      }
      try {
        if (file.bytes > 0 && Files.size(path) != file.bytes) {
          return false;
        }
      } catch (java.io.IOException e) {
        return false;
      }
    }
    return true;
  }

  private void deleteUnitFiles(WriteOptions writeOptions, String unitId) {
    String pattern =
        (writeOptions.filePrefix == null || writeOptions.filePrefix.isEmpty()
                ? ""
                : writeOptions.filePrefix + "-")
            + unitId
            + "-p*.tsfile";
    if (!Files.isDirectory(writeOptions.outDir)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(writeOptions.outDir, pattern)) {
      for (Path path : stream) {
        try {
          Files.deleteIfExists(path);
          LOG.debug("removed incomplete file {}", path.getFileName());
        } catch (java.io.IOException e) {
          LOG.warn("cannot remove incomplete file {}: {}", path, e.getMessage());
        }
      }
    } catch (java.io.IOException e) {
      LOG.debug("cannot scan {}: {}", writeOptions.outDir, e.getMessage());
    }
  }

  private List<MigrationUnit> buildUnits(
      List<TableSpec> specs, long startNanos, long endNanos, Long sliceNanos, InfluxSource source) {
    List<MigrationUnit> units = new ArrayList<>();
    Set<String> used = new HashSet<>();
    for (TableSpec spec : specs) {
      String base = Naming.unique(Naming.sanitize(spec.measurement()), used);
      long from = startNanos;
      long to = endNanos;
      long[] dataRange = source.dataRange(spec.measurement());
      if (dataRange == null && sliceNanos != null && sliceNanos > 0) {
        throw new MigrationException(
            "cannot determine the time range of measurement '"
                + spec.measurement()
                + "', please narrow the range with --start and --end");
      }
      if (dataRange != null) {
        if (dataRange[0] < 0 || dataRange[1] < 0 || dataRange[1] < startNanos || dataRange[0] >= endNanos) {
          LOG.info(
              "measurement {} has no data in [{}, {}), it is skipped",
              spec.measurement(),
              Times.format(startNanos),
              Times.format(endNanos));
          continue;
        }
        from = Math.max(startNanos, dataRange[0]);
        to = Math.min(endNanos, dataRange[1] + 1);
      }
      if (sliceNanos == null || sliceNanos <= 0) {
        units.add(new MigrationUnit(base, spec.measurement(), from, to));
        continue;
      }
      int index = 0;
      for (long sliceStart = from; sliceStart < to; sliceStart += sliceNanos) {
        long sliceEnd = Math.min(sliceStart + sliceNanos, to);
        units.add(
            new MigrationUnit(
                base + "_" + String.format("%05d", index),
                spec.measurement(),
                sliceStart,
                sliceEnd));
        index++;
        if (units.size() > 200_000) {
          throw new MigrationException(
              "--time-slice "
                  + timeSlice
                  + " produces too many slices, please narrow the range with --start and --end");
        }
      }
    }
    return units;
  }

  private void printPlan(
      List<TableSpec> specs,
      List<MigrationUnit> units,
      String retentionPolicy,
      List<String> skipped) {
    LOG.info("retention policy: {}", retentionPolicy);
    for (TableSpec spec : specs) {
      LOG.info(
          "table {} <- measurement {}: tags [{}], fields [{}]",
          spec.tableName(),
          spec.measurement(),
          String.join(",", spec.tagColumns()),
          String.join(",", spec.fieldColumns()));
    }
    LOG.info("{} unit(s) to migrate", units.size());
    if (!skipped.isEmpty()) {
      LOG.warn("{} measurement(s) skipped because of type conflicts: {}", skipped.size(), String.join(",", skipped));
    }
  }

  private void printSummary(
      Manifest manifest, WriteOptions writeOptions, Path manifestFile, List<String> skipped) {
    Map<String, TableStat> totals = new LinkedHashMap<>();
    for (UnitResult unit : manifest.units) {
      if (!"done".equals(unit.status)) {
        continue;
      }
      for (Map.Entry<String, TableStat> entry : unit.tables.entrySet()) {
        TableStat stat = totals.computeIfAbsent(entry.getKey(), key -> new TableStat(key, entry.getValue().measurement));
        stat.rows += entry.getValue().rows;
        stat.devices += entry.getValue().devices;
        if (entry.getValue().minTime != null
            && (stat.minTime == null
                || Times.parseToNanos(entry.getValue().minTime) < Times.parseToNanos(stat.minTime))) {
          stat.minTime = entry.getValue().minTime;
        }
        if (entry.getValue().maxTime != null
            && (stat.maxTime == null
                || Times.parseToNanos(entry.getValue().maxTime) > Times.parseToNanos(stat.maxTime))) {
          stat.maxTime = entry.getValue().maxTime;
        }
      }
    }
    boolean sliced = timeSlice != null && !timeSlice.isEmpty();
    for (TableStat stat : totals.values()) {
      LOG.info(
          "table {}: {} rows, {} series{}, range [{} .. {}]",
          stat.table,
          stat.rows,
          stat.devices,
          sliced ? " (sum of the per-slice counts)" : "",
          stat.minTime == null ? "-" : stat.minTime,
          stat.maxTime == null ? "-" : stat.maxTime);
    }
    LOG.info(
        "migration finished: {} unit(s) done, {} failed, {} rows, {} files, {}",
        manifest.totals.units,
        manifest.totals.failedUnits,
        manifest.totals.rows,
        manifest.totals.files,
        SizeFormat.format(manifest.totals.bytes));
    if (!skipped.isEmpty()) {
      LOG.warn("skipped measurements: {}", String.join(",", skipped));
    }
    LOG.info("manifest: {}", manifestFile);
    LOG.info("tsfile parts: {}", writeOptions.outDir);
  }
}