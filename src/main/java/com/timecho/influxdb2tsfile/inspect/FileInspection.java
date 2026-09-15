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
package com.timecho.influxdb2tsfile.inspect;

import java.util.ArrayList;
import java.util.List;

public class FileInspection {

  public String file;
  public long bytes;
  public long rows;
  public int devices;
  public String minTime;
  public String maxTime;
  public List<TableInspection> tables = new ArrayList<>();

  public static class TableInspection {
    public String table;
    public long rows;
    public int devices;
    public String minTime;
    public String maxTime;
    public List<ColumnInspection> columns = new ArrayList<>();
    public List<String> sample = new ArrayList<>();
  }

  public static class ColumnInspection {
    public String name;
    public String type;
    public String category;

    public ColumnInspection() {}

    public ColumnInspection(String name, String type, String category) {
      this.name = name;
      this.type = type;
      this.category = category;
    }
  }
}
