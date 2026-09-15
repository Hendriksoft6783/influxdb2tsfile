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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.timecho.influxdb2tsfile.core.TableSpec;

final class DeviceBuffer {

  private final TableSpec spec;
  private final String[] tagValues;
  private final List<Row> rows = new ArrayList<>();
  private boolean sorted = true;

  DeviceBuffer(TableSpec spec, String[] tagValues) {
    this.spec = spec;
    this.tagValues = tagValues;
  }

  TableSpec spec() {
    return spec;
  }

  String[] tagValues() {
    return tagValues;
  }

  int size() {
    return rows.size();
  }

  void add(long time, Object[] values) {
    int last = rows.size() - 1;
    if (last >= 0) {
      Row row = rows.get(last);
      if (row.time == time) {
        for (int i = 0; i < values.length; i++) {
          if (values[i] != null) {
            row.values[i] = values[i];
          }
        }
        return;
      }
      if (row.time > time) {
        sorted = false;
      }
    }
    rows.add(new Row(time, values));
  }

  List<Row> drainSorted() {
    if (!sorted) {
      rows.sort(Comparator.comparingLong(row -> row.time));
      sorted = true;
    }
    List<Row> result = new ArrayList<>(rows);
    rows.clear();
    return result;
  }

  static final class Row {
    final long time;
    final Object[] values;

    Row(long time, Object[] values) {
      this.time = time;
      this.values = values;
    }
  }
}
