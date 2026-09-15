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
package com.timecho.influxdb2tsfile.source.influx;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.timecho.influxdb2tsfile.core.InfluxType;
import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.source.MeasurementSchema;
import com.timecho.influxdb2tsfile.source.RowSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfluxSource implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InfluxSource.class);

  private final InfluxClient client;
  private final String database;
  private String retentionPolicy;
  private final int chunkSize;

  public InfluxSource(
      InfluxClient client, String database, String retentionPolicy, int chunkSize) {
    this.client = client;
    this.database = database;
    this.retentionPolicy = retentionPolicy;
    this.chunkSize = chunkSize;
  }

  public ServerInfo ping() {
    return client.ping();
  }

  public String retentionPolicy() {
    return retentionPolicy;
  }

  public void setRetentionPolicy(String retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
  }

  public String defaultRetentionPolicy() {
    final String[] result = {null};
    queryRaw(
        "SHOW RETENTION POLICIES",
        (columns, values) -> {
          CsvHeader header = CsvHeader.of(columns);
          int idxName = header.valueIndex(columns, "name");
          int idxDefault = header.valueIndex(columns, "default");
          if (idxName >= 0
              && idxDefault >= 0
              && idxDefault < values.size()
              && "true".equalsIgnoreCase(values.get(idxDefault))) {
            result[0] = values.get(idxName);
          }
        });
    if (result[0] == null) {
      LOG.debug("cannot determine default retention policy, fall back to autogen");
      result[0] = "autogen";
    }
    return result[0];
  }

  public List<String> showMeasurements() {
    List<String> measurements = new ArrayList<>();
    queryRaw(
        "SHOW MEASUREMENTS",
        (columns, values) -> {
          CsvHeader header = CsvHeader.of(columns);
          int idx = header.valueIndex(columns, "name");
          if (idx < 0) {
            idx = header.firstValueIndex;
          }
          if (idx >= 0 && idx < values.size()) {
            String name = values.get(idx);
            if (name != null && !name.isEmpty()) {
              measurements.add(name);
            }
          }
        });
    return measurements;
  }

  public Map<String, MeasurementSchema> describe(List<String> measurements) {
    Map<String, MeasurementSchema> schemas = new TreeMap<>();
    for (String measurement : measurements) {
      schemas.put(measurement, new MeasurementSchema(measurement));
    }
    try {
      queryRaw("SHOW FIELD KEYS", new FieldKeysHandler(schemas));
      queryRaw("SHOW TAG KEYS", new TagKeysHandler(schemas));
    } catch (MigrationException e) {
      LOG.debug("bulk schema discovery failed ({}), falling back to per measurement queries", e.getMessage());
      for (Map.Entry<String, MeasurementSchema> entry : schemas.entrySet()) {
        String quoted = quote(entry.getKey());
        queryRaw("SHOW FIELD KEYS FROM " + quoted, new FieldKeysHandler(schemas));
        queryRaw("SHOW TAG KEYS FROM " + quoted, new TagKeysHandler(schemas));
      }
    }
    return schemas;
  }

  public Long countPoints(String measurement, String field) {
    if (field == null || field.isEmpty()) {
      return null;
    }
    String statement = "SELECT COUNT(" + quote(field) + ") FROM " + fromClause(measurement);
    final Long[] result = {null};
    queryRaw(
        statement,
        (columns, values) -> {
          SeriesRow row = SeriesRow.of(columns, values);
          if (!row.values.isEmpty()) {
            Object parsed = CsvValueParser.parseBySyntax(row.values.get(0));
            if (parsed instanceof Number) {
              result[0] = ((Number) parsed).longValue();
            }
          }
        });
    return result[0];
  }

  public long[] dataRange(String measurement) {
    try {
      Long first = boundaryTime(measurement, "ASC");
      Long last = boundaryTime(measurement, "DESC");
      return new long[] {first == null ? -1 : first, last == null ? -1 : last};
    } catch (RuntimeException e) {
      LOG.warn("cannot determine the time range of measurement {}: {}", measurement, e.getMessage());
      return null;
    }
  }

  private Long boundaryTime(String measurement, String direction) {
    String statement =
        "SELECT * FROM " + fromClause(measurement) + " ORDER BY time " + direction + " LIMIT 1";
    final Long[] result = {null};
    queryRaw(
        statement,
        (columns, values) -> {
          CsvHeader header = CsvHeader.of(columns);
          if (header.timeIndex >= 0 && header.timeIndex < values.size()) {
            String raw = values.get(header.timeIndex);
            if (!raw.isEmpty()) {
              Object parsed = CsvValueParser.parse(raw, null);
              if (parsed instanceof Long) {
                result[0] = (Long) parsed;
              } else if (parsed instanceof Double) {
                result[0] = ((Double) parsed).longValue();
              } else {
                result[0] = com.timecho.influxdb2tsfile.core.Times.parseToNanos(String.valueOf(parsed));
              }
            }
          }
        });
    return result[0];
  }

  private String fromClause(String measurement) {
    if (retentionPolicy != null && !retentionPolicy.isEmpty()) {
      return quote(retentionPolicy) + "." + quote(measurement);
    }
    return quote(measurement);
  }

  public void query(String measurement, long startNanos, long endNanos, RowSink sink) {
    StringBuilder q = new StringBuilder("SELECT * FROM ");
    q.append(fromClause(measurement));
    q.append(" WHERE time >= ").append(startNanos).append(" AND time < ").append(endNanos);
    q.append(" GROUP BY *");
    Map<String, String> params = new LinkedHashMap<>();
    params.put("q", q.toString());
    params.put("epoch", "ns");
    params.put("chunked", "true");
    params.put("chunk_size", String.valueOf(chunkSize));
    if (retentionPolicy != null && !retentionPolicy.isEmpty()) {
      params.put("rp", retentionPolicy);
    }
    try (InputStream in = client.queryStream(database, params)) {
      InfluxCsvParser.parse(in, new InfluxQueryHandler(measurement, sink));
    } catch (IOException e) {
      throw new MigrationException(
          "failed to read data of measurement '" + measurement + "' from InfluxDB: " + e.getMessage(), e);
    }
  }

  private static final class InfluxQueryHandler implements InfluxCsvParser.Handler {

    private final String measurement;
    private final RowSink sink;
    private List<String> columns;
    private List<String> datatypes;
    private int idxName = -1;
    private int idxTags = -1;
    private int idxTime = -1;

    InfluxQueryHandler(String measurement, RowSink sink) {
      this.measurement = measurement;
      this.sink = sink;
    }

    @Override
    public void header(int headerId, List<String> headerColumns, List<String> headerDatatypes) {
      this.columns = headerColumns;
      this.datatypes = headerDatatypes;
      this.idxName = headerColumns.indexOf("name");
      this.idxTags = headerColumns.indexOf("tags");
      this.idxTime = headerColumns.indexOf("time");
    }

    @Override
    public void row(int headerId, List<String> values) {
      String name =
          idxName >= 0 && idxName < values.size() && !values.get(idxName).isEmpty()
              ? values.get(idxName)
              : measurement;
      Map<String, String> tags = new LinkedHashMap<>();
      if (idxTags >= 0 && idxTags < values.size()) {
        parseTags(values.get(idxTags), tags);
      }
      long timeNanos = 0;
      boolean hasTime = false;
      if (idxTime >= 0 && idxTime < values.size()) {
        String raw = values.get(idxTime);
        if (!raw.isEmpty()) {
          Object parsed = CsvValueParser.parse(raw, datatype(idxTime));
          if (parsed instanceof Long) {
            timeNanos = (Long) parsed;
          } else if (parsed instanceof Double) {
            timeNanos = ((Double) parsed).longValue();
          } else {
            timeNanos = com.timecho.influxdb2tsfile.core.Times.parseToNanos(String.valueOf(parsed));
          }
          hasTime = true;
        }
      }
      if (!hasTime) {
        return;
      }
      Map<String, Object> fields = new LinkedHashMap<>();
      for (int i = 0; i < columns.size(); i++) {
        if (i == idxName || i == idxTags || i == idxTime) {
          continue;
        }
        String raw = values.get(i);
        if (raw.isEmpty()) {
          continue;
        }
        Object value = CsvValueParser.parse(raw, datatype(i));
        if (value != null) {
          fields.put(columns.get(i), value);
        }
      }
      if (fields.isEmpty()) {
        return;
      }
      sink.row(name, tags, timeNanos, fields);
    }

    private String datatype(int index) {
      if (datatypes == null || index >= datatypes.size()) {
        return null;
      }
      return datatypes.get(index);
    }
  }

  private static void parseTags(String raw, Map<String, String> tags) {
    if (raw == null || raw.isEmpty()) {
      return;
    }
    StringBuilder current = new StringBuilder();
    List<String> segments = new ArrayList<>();
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c == '\\' && i + 1 < raw.length()) {
        current.append(c).append(raw.charAt(++i));
        continue;
      }
      if (c == ',') {
        segments.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(c);
    }
    segments.add(current.toString());
    for (String segment : segments) {
      if (segment.isEmpty()) {
        continue;
      }
      int eq = indexOfUnescaped(segment, '=');
      if (eq <= 0) {
        continue;
      }
      tags.put(unescape(segment.substring(0, eq)), unescape(segment.substring(eq + 1)));
    }
  }

  private static int indexOfUnescaped(String text, char target) {
    boolean escaped = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escaped) {
        escaped = false;
        continue;
      }
      if (c == '\\') {
        escaped = true;
        continue;
      }
      if (c == target) {
        return i;
      }
    }
    return -1;
  }

  private static String unescape(String text) {
    if (text.indexOf('\\') < 0) {
      return text;
    }
    StringBuilder sb = new StringBuilder(text.length());
    boolean escaped = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
        continue;
      }
      if (c == '\\') {
        escaped = true;
        continue;
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private static final class FieldKeysHandler implements RawHandler {

    private final Map<String, MeasurementSchema> schemas;

    FieldKeysHandler(Map<String, MeasurementSchema> schemas) {
      this.schemas = schemas;
    }

    @Override
    public void accept(List<String> columns, List<String> values) {
      SeriesRow row = SeriesRow.of(columns, values);
      MeasurementSchema schema = schemas.get(row.seriesName);
      if (schema == null) {
        return;
      }
      if (row.values.size() >= 2) {
        schema.addField(row.values.get(0), InfluxType.fromInfluxName(row.values.get(1)));
      } else if (row.values.size() == 1) {
        schema.addField(row.values.get(0), InfluxType.STRING);
      }
      schema.addRetentionPolicy(row.retentionPolicy);
    }
  }

  private static final class TagKeysHandler implements RawHandler {

    private final Map<String, MeasurementSchema> schemas;

    TagKeysHandler(Map<String, MeasurementSchema> schemas) {
      this.schemas = schemas;
    }

    @Override
    public void accept(List<String> columns, List<String> values) {
      SeriesRow row = SeriesRow.of(columns, values);
      MeasurementSchema schema = schemas.get(row.seriesName);
      if (schema == null) {
        return;
      }
      if (!row.values.isEmpty()) {
        schema.addTagKey(row.values.get(0));
      }
      schema.addRetentionPolicy(row.retentionPolicy);
    }
  }

  private static final class SeriesRow {
    private final String seriesName;
    private final List<String> values = new ArrayList<>();
    private String retentionPolicy;

    private SeriesRow(String seriesName) {
      this.seriesName = seriesName;
    }

    static SeriesRow of(List<String> columns, List<String> cells) {
      int idxName = columns.indexOf("name");
      int idxTags = columns.indexOf("tags");
      int idxTime = columns.indexOf("time");
      String seriesName = idxName >= 0 && idxName < cells.size() ? cells.get(idxName) : "";
      SeriesRow row = new SeriesRow(seriesName);
      if (idxTags >= 0 && idxTags < cells.size()) {
        row.retentionPolicy = parseRetentionPolicy(cells.get(idxTags));
      }
      for (int i = 0; i < columns.size(); i++) {
        if (i == idxName || i == idxTags || i == idxTime) {
          continue;
        }
        String value = i < cells.size() ? cells.get(i) : "";
        if (value != null && !value.isEmpty()) {
          row.values.add(value);
        }
      }
      return row;
    }

    private static String parseRetentionPolicy(String tags) {
      if (tags == null || tags.isEmpty()) {
        return null;
      }
      for (String segment : tags.split(",")) {
        int eq = segment.indexOf('=');
        if (eq > 0 && "rp".equals(unescape(segment.substring(0, eq)))) {
          return unescape(segment.substring(eq + 1));
        }
      }
      return null;
    }
  }

  private interface RawHandler {
    void accept(List<String> columns, List<String> values);
  }

  private static final class HeaderTrackingHandler implements InfluxCsvParser.Handler {

    private final RawHandler delegate;
    private final Map<Integer, List<String>> headers = new LinkedHashMap<>();

    HeaderTrackingHandler(RawHandler delegate) {
      this.delegate = delegate;
    }

    @Override
    public void header(int headerId, List<String> columns, List<String> datatypes) {
      headers.put(headerId, columns);
    }

    @Override
    public void row(int headerId, List<String> values) {
      List<String> columns = headers.get(headerId);
      if (columns != null) {
        delegate.accept(columns, values);
      }
    }
  }

  private void queryRaw(String statement, RawHandler handler) {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("q", statement);
    params.put("epoch", "ns");
    if (retentionPolicy != null && !retentionPolicy.isEmpty() && statement.startsWith("SHOW ")) {
      params.put("rp", retentionPolicy);
    }
    try (InputStream in = client.queryStream(database, params)) {
      InfluxCsvParser.parse(in, new HeaderTrackingHandler(handler));
    } catch (IOException e) {
      throw new MigrationException("failed to execute '" + statement + "': " + e.getMessage(), e);
    }
  }

  static final class CsvHeader {

    final int nameIndex;
    final int tagsIndex;
    final int timeIndex;
    final int firstValueIndex;

    private CsvHeader(int nameIndex, int tagsIndex, int timeIndex, int firstValueIndex) {
      this.nameIndex = nameIndex;
      this.tagsIndex = tagsIndex;
      this.timeIndex = timeIndex;
      this.firstValueIndex = firstValueIndex;
    }

    static CsvHeader of(List<String> columns) {
      int index = 0;
      int nameIndex = -1;
      int tagsIndex = -1;
      int timeIndex = -1;
      if (index < columns.size() && columns.get(index).equalsIgnoreCase("name")) {
        nameIndex = index++;
      }
      if (index < columns.size() && columns.get(index).equalsIgnoreCase("tags")) {
        tagsIndex = index++;
      }
      if (index < columns.size() && columns.get(index).equalsIgnoreCase("time")) {
        timeIndex = index++;
      }
      return new CsvHeader(nameIndex, tagsIndex, timeIndex, index);
    }

    int valueIndex(List<String> columns, String name) {
      for (int i = firstValueIndex; i < columns.size(); i++) {
        if (columns.get(i).equalsIgnoreCase(name)) {
          return i;
        }
      }
      return -1;
    }
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\\\"") + "\"";
  }

  @Override
  public void close() {
    client.close();
  }
}