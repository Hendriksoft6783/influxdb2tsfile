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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import com.timecho.influxdb2tsfile.core.InfluxType;
import com.timecho.influxdb2tsfile.core.SchemaOptions;
import com.timecho.influxdb2tsfile.core.SizeFormat;
import com.timecho.influxdb2tsfile.core.WriteOptions;

import org.apache.tsfile.file.metadata.enums.CompressionType;

import picocli.CommandLine.Option;

public class WriterOptionsMixin {

  @Option(names = {"-o", "--out"}, description = "output directory (default: ${DEFAULT-VALUE})", defaultValue = ".")
  public String out;

  @Option(names = "--file-prefix", description = "prefix of the generated tsfile names", defaultValue = "")
  public String filePrefix;

  @Option(names = "--table-prefix", description = "prefix added to every table name", defaultValue = "")
  public String tablePrefix;

  @Option(names = "--max-file-size", description = "max size of one tsfile part, 0 disables (default: ${DEFAULT-VALUE})", defaultValue = "1GB")
  public String maxFileSize;

  @Option(names = "--max-rows-per-file", description = "max rows of one tsfile part, 0 disables (default: ${DEFAULT-VALUE})", defaultValue = "0")
  public long maxRowsPerFile;

  @Option(names = "--batch-size", description = "rows per tablet flush (default: ${DEFAULT-VALUE})", defaultValue = "4096")
  public int batchSize;

  @Option(names = "--max-buffered-rows", description = "max rows buffered in memory (default: ${DEFAULT-VALUE})", defaultValue = "200000")
  public long maxBufferedRows;

  @Option(names = "--compression", description = "UNCOMPRESSED, SNAPPY, GZIP, LZ4, ZSTD or LZMA2 (default: LZ4, the TsFile default)")
  public String compression;

  @Option(names = "--memory-threshold", description = "memory threshold of one chunk group (default: ${DEFAULT-VALUE})", defaultValue = "64MB")
  public String memoryThreshold;

  @Option(names = "--no-sanitize", description = "keep the original names (lower cased) instead of sanitizing them")
  public boolean noSanitize;

  @Option(names = "--include-tags", split = ",", description = "only these tag keys become TAG columns")
  public List<String> includeTags;

  @Option(names = "--exclude-tags", split = ",", description = "these tag keys are not written")
  public List<String> excludeTags;

  @Option(names = "--exclude-fields", split = ",", description = "these field keys are not written")
  public List<String> excludeFields;

  @Option(
      names = "--field-type",
      split = ",",
      description = "force the type of a field, e.g. --field-type usage=FLOAT (repeatable)")
  public List<String> fieldTypes;

  @Option(
      names = "--type-conflict",
      description = "what to do when a field has more than one type in InfluxDB: fail or text (default: ${DEFAULT-VALUE})",
      defaultValue = "fail")
  public String typeConflict;

  public WriteOptions toWriteOptions() {
    WriteOptions options = new WriteOptions();
    options.outDir = Paths.get(out);
    options.filePrefix = filePrefix == null ? "" : filePrefix;
    options.tablePrefix = tablePrefix == null ? "" : tablePrefix;
    options.maxFileSizeBytes = maxFileSize == null || maxFileSize.isEmpty() ? 0 : SizeFormat.parseBytes(maxFileSize);
    options.maxRowsPerFile = maxRowsPerFile <= 0 ? Long.MAX_VALUE : maxRowsPerFile;
    options.batchSize = batchSize <= 0 ? 4096 : batchSize;
    options.maxBufferedRows = maxBufferedRows <= 0 ? 200_000L : maxBufferedRows;
    options.memoryThresholdBytes = SizeFormat.parseBytes(memoryThreshold);
    options.sanitizeNames = !noSanitize;
    if (compression != null && !compression.isEmpty() && !compression.equalsIgnoreCase("default")) {
      options.compression = CompressionType.valueOf(compression.trim().toUpperCase(Locale.ROOT));
    }
    return options;
  }

  public SchemaOptions toSchemaOptions() {
    SchemaOptions options = new SchemaOptions();
    options.sanitizeNames = !noSanitize;
    options.tablePrefix = tablePrefix == null ? "" : tablePrefix;
    if (includeTags != null && !includeTags.isEmpty()) {
      options.includeTagKeys = new LinkedHashSet<>(includeTags);
    }
    if (excludeTags != null) {
      options.excludeTagKeys = new LinkedHashSet<>(excludeTags);
    }
    if (excludeFields != null) {
      options.excludeFieldKeys = new LinkedHashSet<>(excludeFields);
    }
    if (fieldTypes != null) {
      for (String definition : fieldTypes) {
        int eq = definition.indexOf('=');
        if (eq <= 0) {
          throw new IllegalArgumentException("invalid --field-type value: " + definition + " (expected name=TYPE)");
        }
        String key = definition.substring(0, eq).trim();
        String type = definition.substring(eq + 1).trim();
        options.fieldTypeOverrides.put(key, InfluxType.fromName(type));
        options.fieldTypeOverrides.put(key.toLowerCase(Locale.ROOT), InfluxType.fromName(type));
      }
    }
    options.typeConflictPolicy =
        "text".equalsIgnoreCase(typeConflict)
            ? SchemaOptions.TypeConflictPolicy.TEXT
            : SchemaOptions.TypeConflictPolicy.FAIL;
    return options;
  }

  public List<String> excludedFields() {
    return excludeFields == null ? new ArrayList<>() : excludeFields;
  }
}
