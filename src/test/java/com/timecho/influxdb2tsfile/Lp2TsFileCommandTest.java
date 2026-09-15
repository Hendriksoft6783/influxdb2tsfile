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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.timecho.influxdb2tsfile.cli.Lp2TsFileCommand;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.ManifestStore;
import com.timecho.influxdb2tsfile.support.TsFileRows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import picocli.CommandLine;

class Lp2TsFileCommandTest {

  @TempDir Path tempDir;

  @Test
  void convertsLineProtocolIntoTableModelTsFile() throws Exception {
    Path lp = tempDir.resolve("data.lp");
    Files.write(
        lp,
        ("cpu,host=a,region=cn usage=1.5,count=2i,ok=true 1704067200000000000\n"
                + "cpu,host=a,region=cn usage=2.5 1704067260000000000\n"
                + "cpu,host=b,region=us usage=3.5,count=4i,label=\"x,y\" 1704067200000000000\n")
            .getBytes(StandardCharsets.UTF_8));

    Path out = tempDir.resolve("out");
    int exitCode =
        new CommandLine(new Lp2TsFileCommand())
            .execute(lp.toString(), "-o", out.toString(), "--log-level", "warn");
    assertEquals(0, exitCode);

    Path tsfile = out.resolve("data-p0000.tsfile");
    assertTrue(Files.exists(tsfile));
    List<Map<String, Object>> rows = TsFileRows.read(tsfile.toFile(), "cpu");
    assertEquals(3, rows.size());
    Map<String, Object> first =
        rows.stream().filter(row -> ((Long) row.get("Time")) == 1704067200000000000L && "a".equals(row.get("host")))
            .findFirst()
            .orElseThrow(AssertionError::new);
    assertEquals(1.5d, (Double) first.get("usage"), 0.0001d);
    assertEquals(2L, first.get("count"));
    assertEquals(Boolean.TRUE, first.get("ok"));
    assertNull(first.get("label"));

    Map<String, Object> third =
        rows.stream().filter(row -> "b".equals(row.get("host"))).findFirst().orElseThrow(AssertionError::new);
    assertEquals("x,y", third.get("label"));
    assertEquals(4L, third.get("count"));
    assertNull(third.get("ok"));

    Manifest manifest = ManifestStore.load(out.resolve("manifest.json"));
    assertEquals(1, manifest.units.size());
    assertEquals(3, manifest.totals.rows);
    assertEquals(2, manifest.units.get(0).tables.get("cpu").devices);
  }

  @Test
  void failsOnInvalidLineProtocol() throws IOException {
    Path lp = tempDir.resolve("bad.lp");
    Files.write(lp, "cpu,host=a 1704067200000000000\n".getBytes(StandardCharsets.UTF_8));
    Path out = tempDir.resolve("out-bad");
    int exitCode =
        new CommandLine(new Lp2TsFileCommand())
            .execute(lp.toString(), "-o", out.toString(), "--log-level", "warn");
    assertEquals(1, exitCode);
  }
}