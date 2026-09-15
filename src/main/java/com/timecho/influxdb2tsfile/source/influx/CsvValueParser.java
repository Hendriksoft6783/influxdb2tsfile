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

import com.timecho.influxdb2tsfile.core.MigrationException;

public final class CsvValueParser {

  private CsvValueParser() {}

  public static Object parse(String raw, String datatype) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    if (datatype != null) {
      String type = datatype.trim().toLowerCase();
      switch (type) {
        case "string":
          return raw;
        case "long":
          return parseLong(raw);
        case "unsignedlong":
          return parseUnsigned(raw);
        case "double":
        case "float":
          return parseDouble(raw);
        case "boolean":
          return parseBoolean(raw);
        case "datetime:rfc3339":
        case "datetime:rfc3339nano":
          return com.timecho.influxdb2tsfile.core.Times.parseToNanos(raw);
        default:
          break;
      }
    }
    return parseBySyntax(raw);
  }

  public static Object parseBySyntax(String raw) {
    if (raw.isEmpty()) {
      return null;
    }
    char first = raw.charAt(0);
    if (first == '"' || first == '\'') {
      return raw;
    }
    char last = raw.charAt(raw.length() - 1);
    int digits = last == 'i' || last == 'I' || last == 'u' || last == 'U' ? raw.length() - 1 : 0;
    if (digits > 0 && isNumeric(raw, digits)) {
      return last == 'i' || last == 'I'
          ? (Object) parseLong(raw.substring(0, digits))
          : (Object) parseUnsigned(raw.substring(0, digits));
    }
    if (isBooleanLiteral(raw)) {
      return parseBoolean(raw);
    }
    if (isIntegerLiteral(raw)) {
      return parseLong(raw);
    }
    if (isNumeric(raw, raw.length())) {
      return parseDouble(raw);
    }
    return raw;
  }

  private static boolean isIntegerLiteral(String raw) {
    int start = raw.charAt(0) == '-' || raw.charAt(0) == '+' ? 1 : 0;
    if (start == raw.length()) {
      return false;
    }
    for (int i = start; i < raw.length(); i++) {
      if (!Character.isDigit(raw.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNumeric(String raw, int length) {
    if (length == 0) {
      return false;
    }
    int start = raw.charAt(0) == '-' || raw.charAt(0) == '+' ? 1 : 0;
    if (start == length) {
      return false;
    }
    boolean digitSeen = false;
    for (int i = start; i < length; i++) {
      char c = raw.charAt(i);
      if (Character.isDigit(c)) {
        digitSeen = true;
      } else if (c != '.' && c != 'e' && c != 'E' && c != '-' && c != '+') {
        return false;
      }
    }
    return digitSeen;
  }

  private static boolean isBooleanLiteral(String raw) {
    if (raw.length() == 1) {
      char c = raw.charAt(0);
      return c == 't' || c == 'T' || c == 'f' || c == 'F';
    }
    return raw.equalsIgnoreCase("true") || raw.equalsIgnoreCase("false");
  }

  private static Long parseLong(String raw) {
    try {
      return Long.valueOf(raw);
    } catch (NumberFormatException e) {
      throw new MigrationException("invalid integer value: " + raw);
    }
  }

  private static Long parseUnsigned(String raw) {
    try {
      return Long.parseUnsignedLong(raw);
    } catch (NumberFormatException e) {
      throw new MigrationException("unsigned value out of int64 range: " + raw);
    }
  }

  private static Double parseDouble(String raw) {
    try {
      return Double.valueOf(raw);
    } catch (NumberFormatException e) {
      throw new MigrationException("invalid float value: " + raw);
    }
  }

  private static Boolean parseBoolean(String raw) {
    if (raw.equalsIgnoreCase("true") || raw.equals("t") || raw.equals("T")) {
      return Boolean.TRUE;
    }
    if (raw.equalsIgnoreCase("false") || raw.equals("f") || raw.equals("F")) {
      return Boolean.FALSE;
    }
    throw new MigrationException("invalid boolean value: " + raw);
  }
}
