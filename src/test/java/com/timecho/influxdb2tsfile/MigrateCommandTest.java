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
package com.timecho.influxdb2tsfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.timecho.influxdb2tsfile.cli.MigrateCommand;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.ManifestStore;
import com.timecho.influxdb2tsfile.support.MockInfluxServer;
import com.timecho.influxdb2tsfile.support.TestRows;
import com.timecho.influxdb2tsfile.support.TsFileRows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import picocli.CommandLine;

class MigrateCommandTest {

  private static final long T0 = 1704067200000000000L;
  private static final long HOUR = 3600_000_000_000L;

  private static final Pattern DATA_QUERY =
      Pattern.compile("SELECT \\* FROM \"(\\w+)\".\"(\\w+)\" WHERE time >= (\\d+) AND time < (\\d+) GROUP BY \\*");

  @TempDir Path tempDir;

  private MockInfluxServer server;
  private final List<TestRows.Point> points = new ArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    server = new MockInfluxServer();
    points.clear();
    for (int i = 0; i < 4; i++) {
      for (String host : new String[] {"a", "b"}) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("usage", 1.0 * i + (host.equals("a") ? 0.5 : 1.5));
        fields.put("count", 100L + i);
        points.add(
            TestRows.point(
                "cpu", Map.of("host", host), T0 + i * HOUR, fields));
      }
      Map<String, Object> memFields = new LinkedHashMap<>();
      memFields.put("used", 0.5 * i);
      points.add(TestRows.point("mem", Map.of("host", "a"), T0 + i * HOUR, memFields));
    }
    server.setHandler(this::respond);
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.close();
    }
  }

  private String respond(String query, Map<String, String> params) {
    if (query == null) {
      return null;
    }
    if (query.equals("SHOW MEASUREMENTS")) {
      return "name,tags,name\nmeasurements,,cpu\nmeasurements,,mem\n";
    }
    if (query.equals("SHOW FIELD KEYS")) {
      return "name,tags,fieldKey,fieldType\ncpu,,usage,float\ncpu,,count,integer\nmem,,used,float\n";
    }
    if (query.equals("SHOW TAG KEYS")) {
      return "name,tags,tagKey\ncpu,,host\nmem,,host\n";
    }
    if (query.equals("SHOW RETENTION POLICIES")) {
      return "name,tags,name,duration,shardGroupDuration,replicaN,default\n,,autogen,0s,168h0m0s,1,true\n";
    }
    Matcher boundary = Pattern.compile("SELECT \\* FROM \"(\\w+)\".\"(\\w+)\" ORDER BY time (ASC|DESC) LIMIT 1").matcher(query);
    if (boundary.matches()) {
      String measurement = boundary.group(2);
      long time = boundary.group(3).equals("ASC") ? T0 : T0 + 3 * HOUR;
      return TestRows.toCsv(points, time, time + 1, measurement);
    }
    Matcher data = DATA_QUERY.matcher(query);
    if (data.matches()) {
      return TestRows.toCsv(
          points, Long.parseLong(data.group(3)), Long.parseLong(data.group(4)), data.group(2));
    }
    return null;
  }

  private String[] baseArgs(Path out) {
    return new String[] {
      "--url", server.url(),
      "--database", "telegraf",
      "-o", out.toString(),
      "--log-level", "warn"
    };
  }

  @Test
  void migratesWithTimeSlicesAndVerifies() throws Exception {
    Path out = tempDir.resolve("out");
    List<String> args = new ArrayList<>(List.of(baseArgs(out)));
    args.addAll(List.of("--time-slice", "2h", "--parallel", "2", "--verify"));
    int exitCode = new CommandLine(new MigrateCommand()).execute(args.toArray(new String[0]));
    assertEquals(0, exitCode);

    List<Map<String, Object>> cpuRows = readTable(out, "cpu");
    assertEquals(8, cpuRows.size());
    List<Map<String, Object>> memRows = readTable(out, "mem");
    assertEquals(4, memRows.size());

    Map<String, Object> rowA =
        cpuRows.stream()
            .filter(row -> ((Long) row.get("Time")) == T0 && "a".equals(row.get("host")))
            .findFirst()
            .orElseThrow(AssertionError::new);
    assertEquals(0.5d, (Double) rowA.get("usage"), 0.0001d);
    assertEquals(100L, rowA.get("count"));

    Manifest manifest = ManifestStore.load(out.resolve("manifest.json"));
    assertEquals(4, manifest.units.size());
    assertEquals(12, manifest.totals.rows);
    assertEquals("cpu", manifest.tables.get(0).table);
    assertTrue(
        manifest.tables.get(0).columns.stream().anyMatch(column -> "usage".equals(column.source)));
  }

  @Test
  void migratesOnlyTheSelectedMeasurements() throws Exception {
    Path out = tempDir.resolve("selected");
    List<String> args = new ArrayList<>(List.of(baseArgs(out)));
    args.addAll(List.of("--measurements", "cpu"));
    int exitCode = new CommandLine(new MigrateCommand()).execute(args.toArray(new String[0]));
    assertEquals(0, exitCode);
    try (var stream = Files.list(out)) {
      List<String> names =
          stream
              .map(path -> path.getFileName().toString())
              .filter(name -> name.endsWith(".tsfile"))
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, names.size());
      assertTrue(names.get(0).startsWith("cpu"));
    }
    Manifest manifest = ManifestStore.load(out.resolve("manifest.json"));
    assertEquals(1, manifest.measurements.size());
    assertEquals(8, manifest.totals.rows);
  }

  @Test
  void resumesAfterAFailedUnitWithoutWritingDuplicates() throws Exception {
    Path out = tempDir.resolve("resume");
    List<String> args = new ArrayList<>(List.of(baseArgs(out)));
    args.addAll(List.of("--time-slice", "2h", "--retries", "0"));
    server.failNextDataQueries(1);
    int failed = new CommandLine(new MigrateCommand()).execute(args.toArray(new String[0]));
    assertEquals(1, failed);

    Manifest afterFailure = ManifestStore.load(out.resolve("manifest.json"));
    assertEquals(1, afterFailure.totals.failedUnits);

    List<String> resumeArgs = new ArrayList<>(List.of(baseArgs(out)));
    resumeArgs.addAll(List.of("--time-slice", "2h", "--resume", "--verify"));
    int exitCode = new CommandLine(new MigrateCommand()).execute(resumeArgs.toArray(new String[0]));
    assertEquals(0, exitCode);

    List<Map<String, Object>> cpuRows = readTable(out, "cpu");
    assertEquals(8, cpuRows.size());
    List<Map<String, Object>> memRows = readTable(out, "mem");
    assertEquals(4, memRows.size());
    Manifest manifest = ManifestStore.load(out.resolve("manifest.json"));
    assertEquals(0, manifest.totals.failedUnits);
    assertEquals(12, manifest.totals.rows);
  }

  private List<Map<String, Object>> readTable(Path out, String table) throws Exception {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (var stream = Files.list(out)) {
      for (Path file : stream.filter(path -> path.getFileName().toString().endsWith(".tsfile")).collect(java.util.stream.Collectors.toList())) {
        if (!file.getFileName().toString().startsWith(table)) {
          continue;
        }
        rows.addAll(TsFileRows.read(file.toFile(), table));
      }
    }
    assertNotNull(rows);
    assertTrue(rows.size() > 0);
    return rows;
  }
}