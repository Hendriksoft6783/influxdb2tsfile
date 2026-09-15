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
package com.timecho.influxdb2tsfile.source.lp;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;
import com.timecho.influxdb2tsfile.source.RowSink;

public final class LineProtocolSource implements Closeable {

  private final List<Path> files;
  private final LineProtocolParser parser;
  private boolean skipInvalid;
  private final List<String> invalidSamples = new ArrayList<>();
  private long invalidLines;
  private long lineCount;
  private long pointCount;

  public LineProtocolSource(List<Path> files, LineProtocolParser parser) {
    this.files = files;
    this.parser = parser;
  }

  public void setSkipInvalid(boolean skipInvalid) {
    this.skipInvalid = skipInvalid;
  }

  public long invalidLines() {
    return invalidLines;
  }

  public List<String> invalidSamples() {
    return invalidSamples;
  }

  public List<MeasurementSchema> discover() throws IOException {
    Map<String, MeasurementSchema> schemas = new LinkedHashMap<>();
    forEachPoint(
        (measurement, tags, fields, timeNanos) -> {
          MeasurementSchema schema =
              schemas.computeIfAbsent(measurement, MeasurementSchema::new);
          for (Map.Entry<String, String> tag : tags.entrySet()) {
            schema.addTagKey(tag.getKey());
          }
          for (Map.Entry<String, Object> field : fields.entrySet()) {
            schema.addField(field.getKey(), typeOf(field.getValue()));
          }
        });
    return new ArrayList<>(schemas.values());
  }

  public void scanAll(RowSink sink) throws IOException {
    forEachPoint(
        (measurement, tags, fields, timeNanos) -> sink.row(measurement, tags, timeNanos, fields));
  }

  public long lineCount() {
    return lineCount;
  }

  public long pointCount() {
    return pointCount;
  }

  private static com.timecho.influxdb2tsfile.core.InfluxType typeOf(Object value) {
    if (value instanceof Long) {
      return com.timecho.influxdb2tsfile.core.InfluxType.INTEGER;
    }
    if (value instanceof Double) {
      return com.timecho.influxdb2tsfile.core.InfluxType.FLOAT;
    }
    if (value instanceof Boolean) {
      return com.timecho.influxdb2tsfile.core.InfluxType.BOOLEAN;
    }
    return com.timecho.influxdb2tsfile.core.InfluxType.STRING;
  }

  private void forEachPoint(LineProtocolParser.PointHandler handler) throws IOException {
    for (Path file : files) {
      try (BufferedReader reader =
          Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
        String line;
        long lineNumber = 0;
        while ((line = reader.readLine()) != null) {
          lineCount++;
          lineNumber++;
          try {
            if (parser.parseLine(line, handler)) {
              pointCount++;
            }
          } catch (RuntimeException e) {
            if (!skipInvalid) {
              throw new MigrationException(
                  file + ":" + lineNumber + ": " + e.getMessage(), e);
            }
            invalidLines++;
            if (invalidSamples.size() < 5) {
              invalidSamples.add(file.getFileName() + ":" + lineNumber + ": " + e.getMessage());
            }
          }
        }
      }
    }
  }

  @Override
  public void close() {}
}
