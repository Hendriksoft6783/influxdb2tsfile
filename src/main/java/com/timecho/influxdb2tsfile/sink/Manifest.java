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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Manifest {

  public String tool;
  public String version;
  public String tsfileVersion;
  public String createdAt;
  public String sourceType;
  public String url;
  public String database;
  public List<String> measurements = new ArrayList<>();
  public Map<String, String> options = new LinkedHashMap<>();
  public List<TableRecord> tables = new ArrayList<>();
  public List<UnitResult> units = new ArrayList<>();
  public Totals totals = new Totals();

  public static class Totals {
    public long rows;
    public long files;
    public long bytes;
    public int units;
    public int failedUnits;
  }

  public static class TableRecord {
    public String table;
    public String measurement;
    public List<ColumnRecord> columns = new ArrayList<>();
  }

  public static class ColumnRecord {
    public String column;
    public String source;
    public String type;
    public String category;
  }

  public void recomputeTotals() {
    totals = new Totals();
    for (UnitResult unit : units) {
      totals.rows += unit.rows;
      totals.files += unit.files.size();
      totals.bytes += unit.bytes();
      if ("done".equals(unit.status) || "empty".equals(unit.status)) {
        totals.units++;
      } else {
        totals.failedUnits++;
      }
    }
  }
}
