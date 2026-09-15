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
package com.timecho.influxdb2tsfile.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TestRows {

  public static final class Point {
    public String measurement;
    public Map<String, String> tags = new LinkedHashMap<>();
    public long timeNanos;
    public Map<String, Object> fields = new LinkedHashMap<>();
  }

  public final class Csv {
    public static final String HEADER_PREFIX = "name,tags,";
  }

  private TestRows() {}

  public static Point point(
      String measurement, Map<String, String> tags, long timeNanos, Map<String, Object> fields) {
    Point point = new Point();
    point.measurement = measurement;
    point.tags = tags;
    point.timeNanos = timeNanos;
    point.fields = fields;
    return point;
  }

  public static String toCsv(List<Point> points, long startNanos, long endNanos, String measurement) {
    Map<String, List<Point>> bySeries = new LinkedHashMap<>();
    List<String> fieldKeys = new ArrayList<>();
    for (Point point : points) {
      if (!point.measurement.equals(measurement)) {
        continue;
      }
      if (point.timeNanos < startNanos || point.timeNanos >= endNanos) {
        continue;
      }
      StringBuilder key = new StringBuilder();
      for (Map.Entry<String, String> tag : point.tags.entrySet()) {
        key.append(tag.getKey()).append('=').append(tag.getValue()).append(',');
      }
      bySeries.computeIfAbsent(key.toString(), ignored -> new ArrayList<>()).add(point);
      for (String field : point.fields.keySet()) {
        if (!fieldKeys.contains(field)) {
          fieldKeys.add(field);
        }
      }
    }
    if (bySeries.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("name,tags,time");
    for (String field : fieldKeys) {
      sb.append(',').append(field);
    }
    sb.append('\n');
    for (Map.Entry<String, List<Point>> series : bySeries.entrySet()) {
      for (Point point : series.getValue()) {
        sb.append(point.measurement).append(',');
        sb.append('"').append(series.getKey().replaceAll(",$", "")).append('"');
        sb.append(',').append(point.timeNanos);
        for (String field : fieldKeys) {
          sb.append(',');
          Object value = point.fields.get(field);
          if (value != null) {
            sb.append(format(value));
          }
        }
        sb.append('\n');
      }
    }
    return sb.toString();
  }

  private static String format(Object value) {
    if (value instanceof Long) {
      return value + "i";
    }
    if (value instanceof String) {
      return "\"" + value + "\"";
    }
    return String.valueOf(value);
  }
}
