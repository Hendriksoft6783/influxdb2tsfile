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
package com.timecho.influxdb2tsfile.core;

public final class SizeFormat {

  private SizeFormat() {}

  public static long parseBytes(String text) {
    String s = text.trim().toLowerCase();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("empty size");
    }
    int i = 0;
    while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) {
      i++;
    }
    if (i == 0) {
      throw new IllegalArgumentException("invalid size: " + text);
    }
    double value = Double.parseDouble(s.substring(0, i));
    String unit = s.substring(i).trim();
    long multiplier;
    switch (unit) {
      case "":
      case "b":
        multiplier = 1L;
        break;
      case "k":
      case "kb":
      case "kib":
        multiplier = 1024L;
        break;
      case "m":
      case "mb":
      case "mib":
        multiplier = 1024L * 1024L;
        break;
      case "g":
      case "gb":
      case "gib":
        multiplier = 1024L * 1024L * 1024L;
        break;
      case "t":
      case "tb":
      case "tib":
        multiplier = 1024L * 1024L * 1024L * 1024L;
        break;
      default:
        throw new IllegalArgumentException("invalid size unit: " + unit);
    }
    return (long) (value * multiplier);
  }

  public static String format(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    String[] units = {"KB", "MB", "GB", "TB", "PB"};
    double value = bytes;
    int idx = -1;
    while (value >= 1024 && idx < units.length - 1) {
      value /= 1024;
      idx++;
    }
    return String.format("%.2f %s", value, units[idx]);
  }
}
