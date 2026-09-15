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
package com.timecho.influxdb2tsfile.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import com.timecho.influxdb2tsfile.inspect.FileInspection;
import com.timecho.influxdb2tsfile.inspect.TsFileInspector;
import com.timecho.influxdb2tsfile.sink.Manifest;
import com.timecho.influxdb2tsfile.sink.UnitFile;
import com.timecho.influxdb2tsfile.sink.UnitResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Verifier {

  private static final Logger LOG = LoggerFactory.getLogger(Verifier.class);

  private Verifier() {}

  public static boolean verify(Manifest manifest, Path outDir) {
    Map<String, Long> expected = ManifestSupport.rowsByTable(manifest);
    Map<String, Long> actual = new LinkedHashMap<>();
    int missing = 0;
    for (UnitResult unit : manifest.units) {
      if (!"done".equals(unit.status)) {
        continue;
      }
      for (UnitFile file : unit.files) {
        Path path = outDir.resolve(file.name);
        if (!Files.exists(path)) {
          LOG.error("missing tsfile part: {}", path);
          missing++;
          continue;
        }
        try {
          FileInspection inspection = TsFileInspector.inspect(path, 0);
          for (FileInspection.TableInspection table : inspection.tables) {
            actual.merge(table.table, table.rows, Long::sum);
          }
        } catch (IOException e) {
          LOG.error("cannot read tsfile {}: {}", path, e.getMessage());
          missing++;
        }
      }
    }
    boolean ok = missing == 0 && expected.size() == actual.size();
    for (Map.Entry<String, Long> entry : expected.entrySet()) {
      Long found = actual.get(entry.getKey());
      if (found == null || !found.equals(entry.getValue())) {
        ok = false;
        LOG.error(
            "row count mismatch for table {}: written={}, read={}",
            entry.getKey(),
            entry.getValue(),
            found == null ? 0 : found);
      }
    }
    for (Map.Entry<String, Long> entry : actual.entrySet()) {
      if (!expected.containsKey(entry.getKey())) {
        ok = false;
        LOG.error("table {} exists in the tsfile parts but not in the manifest", entry.getKey());
      }
    }
    if (ok) {
      long rows = 0;
      for (Long value : expected.values()) {
        rows += value;
      }
      LOG.info("verification passed: {} rows in {} tables", rows, expected.size());
    } else {
      LOG.error("verification failed");
    }
    return ok;
  }
}
