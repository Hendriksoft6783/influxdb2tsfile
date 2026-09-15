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

import java.util.LinkedHashMap;
import java.util.Map;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.TimePrecision;

public final class LineProtocolParser {

  public interface PointHandler {
    void point(String measurement, Map<String, String> tags, Map<String, Object> fields, long timeNanos);
  }

  private final TimePrecision precision;
  private final Long defaultTimeNanos;

  public LineProtocolParser(TimePrecision precision, Long defaultTimeNanos) {
    this.precision = precision;
    this.defaultTimeNanos = defaultTimeNanos;
  }

  public boolean parseLine(String line, PointHandler handler) {
    if (line == null) {
      return false;
    }
    String text = line.trim();
    if (text.isEmpty() || text.startsWith("#")) {
      return false;
    }
    int firstSpace = -1;
    int secondSpace = -1;
    boolean inQuotes = false;
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
      if (c == '"') {
        inQuotes = !inQuotes;
        continue;
      }
      if (c == ' ' && !inQuotes) {
        if (firstSpace < 0) {
          firstSpace = i;
        } else {
          secondSpace = i;
          break;
        }
      }
    }
    if (firstSpace < 0) {
      throw new MigrationException("invalid line protocol line, no field set: " + abbreviate(text));
    }
    String head = text.substring(0, firstSpace);
    String fieldPart =
        secondSpace < 0 ? text.substring(firstSpace + 1) : text.substring(firstSpace + 1, secondSpace);
    String timePart = secondSpace < 0 ? null : text.substring(secondSpace + 1).trim();

    int[] segmentStart = {0};
    String measurement = unescapeKey(nextSegment(head, segmentStart, ','));
    if (measurement.isEmpty()) {
      throw new MigrationException("invalid line protocol line, empty measurement: " + abbreviate(text));
    }
    Map<String, String> tags = new LinkedHashMap<>();
    String tagSegment;
    while (!(tagSegment = nextSegment(head, segmentStart, ',')).isEmpty()) {
      int eq = indexOfUnescaped(tagSegment, '=');
      if (eq <= 0) {
        continue;
      }
      tags.put(unescapeKey(tagSegment.substring(0, eq)), unescapeKey(tagSegment.substring(eq + 1)));
    }
    Map<String, Object> fields = new LinkedHashMap<>();
    int[] fieldStart = {0};
    String fieldSegment;
    while (!(fieldSegment = nextFieldSegment(fieldPart, fieldStart)).isEmpty()) {
      int eq = indexOfUnescapedOutsideQuotes(fieldSegment, '=');
      if (eq <= 0) {
        continue;
      }
      String key = unescapeKey(fieldSegment.substring(0, eq));
      String rawValue = fieldSegment.substring(eq + 1).trim();
      if (rawValue.isEmpty()) {
        continue;
      }
      fields.put(key, parseValue(rawValue));
    }
    if (fields.isEmpty()) {
      throw new MigrationException(
          "invalid line protocol line, no field set (a space is required between tags and fields): "
              + abbreviate(text));
    }
    long timeNanos;
    if (timePart == null || timePart.isEmpty()) {
      if (defaultTimeNanos == null) {
        throw new MigrationException(
            "line protocol line without timestamp, use --default-time to supply one: " + abbreviate(text));
      }
      timeNanos = defaultTimeNanos;
    } else {
      timeNanos = precision.toNanos(Long.parseLong(timePart));
    }
    handler.point(measurement, tags, fields, timeNanos);
    return true;
  }

  public static Object parseValue(String raw) {
    char first = raw.charAt(0);
    if (first == '"') {
      if (raw.length() < 2 || raw.charAt(raw.length() - 1) != '"') {
        throw new MigrationException("unterminated string value: " + raw);
      }
      return unescapeStringValue(raw.substring(1, raw.length() - 1));
    }
    char last = raw.charAt(raw.length() - 1);
    if (last == 'i' || last == 'I') {
      return Long.parseLong(raw.substring(0, raw.length() - 1));
    }
    if (last == 'u' || last == 'U') {
      String digits = raw.substring(0, raw.length() - 1);
      long value;
      try {
        value = Long.parseUnsignedLong(digits);
      } catch (NumberFormatException e) {
        throw new MigrationException("unsigned integer out of int64 range: " + raw);
      }
      return value;
    }
    if (isBoolean(raw)) {
      return booleanOf(raw);
    }
    try {
      return Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      throw new MigrationException(
          "invalid field value '"
              + raw
              + "': numbers must not be quoted and string values must be wrapped in double quotes (\"value\")");
    }
  }

  private static boolean isBoolean(String raw) {
    return raw.length() <= 5
        && (raw.equalsIgnoreCase("true")
            || raw.equalsIgnoreCase("false")
            || raw.equals("t")
            || raw.equals("T")
            || raw.equals("f")
            || raw.equals("F"));
  }

  private static boolean booleanOf(String raw) {
    return raw.equalsIgnoreCase("true") || raw.equals("t") || raw.equals("T");
  }

  private static String nextSegment(String text, int[] start, char separator) {
    int from = start[0];
    if (from >= text.length()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    for (int i = from; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
        continue;
      }
      if (c == '\\') {
        sb.append(c);
        escaped = true;
        continue;
      }
      if (c == separator) {
        start[0] = i + 1;
        return sb.toString();
      }
      sb.append(c);
    }
    start[0] = text.length();
    return sb.toString();
  }

  private static String nextFieldSegment(String text, int[] start) {
    int from = start[0];
    if (from >= text.length()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    boolean inQuotes = false;
    boolean escaped = false;
    for (int i = from; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
        continue;
      }
      if (c == '\\') {
        sb.append(c);
        escaped = true;
        continue;
      }
      if (c == '"') {
        inQuotes = !inQuotes;
        sb.append(c);
        continue;
      }
      if (c == ',' && !inQuotes) {
        start[0] = i + 1;
        return sb.toString().trim();
      }
      sb.append(c);
    }
    start[0] = text.length();
    return sb.toString().trim();
  }

  static int indexOfUnescaped(String text, char target) {
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

  static int indexOfUnescapedOutsideQuotes(String text, char target) {
    boolean escaped = false;
    boolean inQuotes = false;
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
      if (c == '"') {
        inQuotes = !inQuotes;
        continue;
      }
      if (c == target && !inQuotes) {
        return i;
      }
    }
    return -1;
  }

  static String unescapeKey(String text) {
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
    if (escaped) {
      sb.append('\\');
    }
    return sb.toString();
  }

  static String unescapeStringValue(String text) {
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
    if (escaped) {
      sb.append('\\');
    }
    return sb.toString();
  }

  private static String abbreviate(String text) {
    return text.length() <= 120 ? text : text.substring(0, 120) + "...";
  }
}
