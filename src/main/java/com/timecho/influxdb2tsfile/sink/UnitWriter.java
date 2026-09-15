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
package com.timecho.influxdb2tsfile.sink;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.Progress;
import com.timecho.influxdb2tsfile.core.TableSpec;
import com.timecho.influxdb2tsfile.core.Times;
import com.timecho.influxdb2tsfile.core.WriteOptions;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UnitWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(UnitWriter.class);
  private static final char SEP = (char) 1;

  private final Path outDir;
  private final WriteOptions options;
  private final String unitId;
  private final Progress progress;
  private final Map<String, TableSpec> specByMeasurement = new LinkedHashMap<>();
  private final Map<String, TableStat> tableStats = new LinkedHashMap<>();
  private final Map<String, long[]> tableTimeRange = new LinkedHashMap<>();
  private final Map<String, DeviceBuffer> buffers = new LinkedHashMap<>();
  private final List<UnitFile> files = new ArrayList<>();

  private TsFileWriter writer;
  private Path currentFile;
  private int partIndex;
  private long rowsInFile;
  private long fileMinTime = Long.MAX_VALUE;
  private long fileMaxTime = Long.MIN_VALUE;
  private long rows;
  private long bufferedRows;
  private long minTime = Long.MAX_VALUE;
  private long maxTime = Long.MIN_VALUE;
  private String lastSeriesKey;
  private String[] lastTagValues;
  private final long startedAt = System.currentTimeMillis();

  public UnitWriter(
      Path outDir, WriteOptions options, String unitId, List<TableSpec> specs, Progress progress) {
    this.outDir = outDir;
    this.options = options;
    this.unitId = unitId;
    this.progress = progress;
    for (TableSpec spec : specs) {
      specByMeasurement.put(spec.measurement(), spec);
      tableStats.put(spec.tableName(), new TableStat(spec.tableName(), spec.measurement()));
    }
  }

  public void addRow(String measurement, Map<String, String> tags, long timeNanos, Map<String, Object> fields) {
    if (fields == null || fields.isEmpty()) {
      return;
    }
    TableSpec spec = specByMeasurement.get(measurement);
    if (spec == null) {
      return;
    }
    Object[] values = new Object[spec.fieldCount()];
    boolean any = false;
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      int index = spec.fieldIndex(entry.getKey());
      if (index < 0) {
        continue;
      }
      values[index] = spec.coerce(index, entry.getValue());
      any = true;
    }
    if (!any) {
      return;
    }
    String[] tagValues = resolveTags(spec, tags);
    String deviceKey = deviceKey(spec, tagValues);
    DeviceBuffer buffer = buffers.get(deviceKey);
    if (buffer == null) {
      buffer = new DeviceBuffer(spec, tagValues);
      buffers.put(deviceKey, buffer);
      tableStats.get(spec.tableName()).devices++;
    }
    buffer.add(timeNanos, values);
    bufferedRows++;
    if (buffer.size() >= options.batchSize) {
      flushBuffer(buffer);
    } else if (bufferedRows >= options.maxBufferedRows) {
      flushLargestBuffers();
    }
  }

  private String[] resolveTags(TableSpec spec, Map<String, String> tags) {
    String key = seriesKey(spec, tags);
    if (key.equals(lastSeriesKey)) {
      return lastTagValues;
    }
    String[] values = spec.tagValues(tags);
    lastSeriesKey = key;
    lastTagValues = values;
    return values;
  }

  private String seriesKey(TableSpec spec, Map<String, String> tags) {
    if (tags == null || tags.isEmpty() || spec.tagCount() == 0) {
      return spec.tableName();
    }
    StringBuilder sb = new StringBuilder(spec.tableName());
    for (String tagKey : spec.tagKeys()) {
      String value = tags.get(tagKey);
      sb.append(SEP).append(value == null ? "" : value);
    }
    return sb.toString();
  }

  private String deviceKey(TableSpec spec, String[] tagValues) {
    if (tagValues.length == 0) {
      return spec.tableName();
    }
    StringBuilder sb = new StringBuilder(spec.tableName());
    for (String value : tagValues) {
      sb.append(SEP).append(value);
    }
    return sb.toString();
  }

  private void flushLargestBuffers() {
    List<DeviceBuffer> pending = new ArrayList<>();
    for (DeviceBuffer buffer : buffers.values()) {
      if (buffer.size() > 0) {
        pending.add(buffer);
      }
    }
    pending.sort(Comparator.comparingInt(DeviceBuffer::size).reversed());
    for (DeviceBuffer buffer : pending) {
      if (bufferedRows < options.maxBufferedRows) {
        return;
      }
      flushBuffer(buffer);
    }
  }

  private void flushBuffer(DeviceBuffer buffer) {
    if (buffer.size() == 0) {
      return;
    }
    List<DeviceBuffer.Row> rowsToWrite = buffer.drainSorted();
    bufferedRows -= rowsToWrite.size();
    ensureWriter();
    TableSpec spec = buffer.spec();
    Tablet tablet = spec.newTablet(rowsToWrite.size());
    String[] tagValues = buffer.tagValues();
    TableStat stat = tableStats.get(spec.tableName());
    for (int r = 0; r < rowsToWrite.size(); r++) {
      DeviceBuffer.Row row = rowsToWrite.get(r);
      tablet.addTimestamp(r, row.time);
      for (int t = 0; t < tagValues.length; t++) {
        tablet.addValue(spec.tagColumns().get(t), r, tagValues[t]);
      }
      for (int c = 0; c < row.values.length; c++) {
        Object value = row.values[c];
        if (value != null) {
          tablet.addValue(spec.fieldColumns().get(c), r, value);
        }
      }
      if (row.time < minTime) {
        minTime = row.time;
      }
      if (row.time > maxTime) {
        maxTime = row.time;
      }
      if (row.time < fileMinTime) {
        fileMinTime = row.time;
      }
      if (row.time > fileMaxTime) {
        fileMaxTime = row.time;
      }
      long[] range = tableTimeRange.computeIfAbsent(spec.tableName(), key -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      if (row.time < range[0]) {
        range[0] = row.time;
      }
      if (row.time > range[1]) {
        range[1] = row.time;
      }
      stat.rows++;
    }
    try {
      writer.writeTable(tablet, devicePair(spec, tagValues, rowsToWrite.size()));
    } catch (IOException | WriteProcessException e) {
      throw new MigrationException("failed to write rows of " + unitId + " into " + currentFile, e);
    }
    rowsInFile += rowsToWrite.size();
    rows += rowsToWrite.size();
    progress.addRows(rowsToWrite.size());
    progress.report();
    maybeRollFile();
  }

  private List<Pair<IDeviceID, Integer>> devicePair(TableSpec spec, String[] tagValues, int rowCount) {
    String[] ids = new String[tagValues.length + 1];
    ids[0] = spec.tableName();
    System.arraycopy(tagValues, 0, ids, 1, tagValues.length);
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(ids);
    return Collections.singletonList(new Pair<>(deviceID, rowCount));
  }

  private void ensureWriter() {
    if (writer != null) {
      return;
    }
    currentFile = outDir.resolve(options.partFileName(unitId, partIndex));
    try {
      Files.createDirectories(outDir);
      TsFileWriter newWriter = new TsFileWriter(new File(currentFile.toString()));
      newWriter.setMemoryThreshold((int) Math.max(options.memoryThresholdBytes, 1024L * 1024L));
      for (TableSpec spec : specByMeasurement.values()) {
        newWriter.registerTableSchema(spec.toTableSchema(options.compression));
      }
      writer = newWriter;
    } catch (IOException e) {
      throw new MigrationException("cannot create tsfile " + currentFile, e);
    }
    rowsInFile = 0;
    fileMinTime = Long.MAX_VALUE;
    fileMaxTime = Long.MIN_VALUE;
    LOG.debug("unit {} writes to {}", unitId, currentFile.getFileName());
  }

  private void maybeRollFile() {
    boolean roll = false;
    if (options.maxRowsPerFile > 0 && rowsInFile >= options.maxRowsPerFile) {
      roll = true;
    } else if (options.maxFileSizeBytes > 0 && currentFile != null) {
      try {
        roll = Files.size(currentFile) >= options.maxFileSizeBytes;
      } catch (IOException e) {
        LOG.debug("cannot read size of {}", currentFile, e);
      }
    }
    if (roll) {
      closeCurrentWriter();
      partIndex++;
    }
  }

  private void closeCurrentWriter() {
    if (writer == null) {
      return;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new MigrationException("cannot close tsfile " + currentFile, e);
    }
    writer = null;
    long size = 0;
    try {
      size = Files.size(currentFile);
    } catch (IOException e) {
      LOG.debug("cannot read size of {}", currentFile, e);
    }
    if (rowsInFile > 0 || size > 0) {
      files.add(
          new UnitFile(
              currentFile.getFileName().toString(),
              rowsInFile,
              size,
              fileMinTime == Long.MAX_VALUE ? null : Times.format(fileMinTime),
              fileMaxTime == Long.MIN_VALUE ? null : Times.format(fileMaxTime)));
    }
    progress.addFile(size);
  }

  public long rows() {
    return rows;
  }

  public UnitResult result() {
    UnitResult result = new UnitResult();
    result.unitId = unitId;
    result.rows = rows;
    result.files = new ArrayList<>(files);
    result.tables = new LinkedHashMap<>();
    for (Map.Entry<String, TableStat> entry : tableStats.entrySet()) {
      if (entry.getValue().rows > 0) {
        result.tables.put(entry.getKey(), entry.getValue());
      }
    }
    for (Map.Entry<String, long[]> entry : tableTimeRange.entrySet()) {
      TableStat stat = result.tables.get(entry.getKey());
      if (stat != null) {
        stat.minTime = entry.getValue()[0] == Long.MAX_VALUE ? null : Times.format(entry.getValue()[0]);
        stat.maxTime = entry.getValue()[1] == Long.MIN_VALUE ? null : Times.format(entry.getValue()[1]);
      }
    }
    result.minTime = minTime == Long.MAX_VALUE ? null : Times.format(minTime);
    result.maxTime = maxTime == Long.MIN_VALUE ? null : Times.format(maxTime);
    result.elapsedMillis = System.currentTimeMillis() - startedAt;
    return result;
  }

  @Override
  public void close() {
    for (DeviceBuffer buffer : new ArrayList<>(buffers.values())) {
      flushBuffer(buffer);
    }
    closeCurrentWriter();
  }
}
