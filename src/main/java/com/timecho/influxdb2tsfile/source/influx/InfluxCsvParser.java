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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.timecho.influxdb2tsfile.core.MigrationException;

public final class InfluxCsvParser {

  public interface Handler {
    void header(int headerId, List<String> columns, List<String> datatypes);

    void row(int headerId, List<String> values);
  }

  private InfluxCsvParser() {}

  public static void parse(InputStream in, Handler handler) throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
    List<String> datatypes = Collections.emptyList();
    List<String> columns = null;
    String rawHeader = null;
    int headerId = -1;
    String line;
    while ((line = reader.readLine()) != null) {
      if (!line.isEmpty() && line.charAt(line.length() - 1) == '\r') {
        line = line.substring(0, line.length() - 1);
      }
      if (line.isEmpty()) {
        columns = null;
        rawHeader = null;
        datatypes = Collections.emptyList();
        continue;
      }
      if (line.charAt(0) == '#') {
        List<String> cells = CsvLineParser.parse(line);
        if (!cells.isEmpty() && cells.get(0).equalsIgnoreCase("#datatype")) {
          List<String> types = new ArrayList<>(cells.size());
          types.add("");
          for (int i = 1; i < cells.size(); i++) {
            types.add(cells.get(i));
          }
          datatypes = types;
        }
        continue;
      }
      if (line.indexOf('{') == 0 && line.contains("\"error\"")) {
        throw new MigrationException("InfluxDB returned an error while streaming the result: " + line);
      }
      if (columns == null) {
        columns = CsvLineParser.parse(line);
        rawHeader = line;
        headerId++;
        handler.header(headerId, columns, datatypes);
        continue;
      }
      if (line.equals(rawHeader)) {
        continue;
      }
      List<String> values = CsvLineParser.parse(line);
      if (values.size() != columns.size()) {
        throw new MigrationException(
            "unexpected CSV row with " + values.size() + " columns, expected " + columns.size() + ": " + line);
      }
      handler.row(headerId, values);
    }
  }
}
