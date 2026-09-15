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

import java.time.Duration;

public final class DurationFormat {

  private DurationFormat() {}

  public static Duration parse(String text) {
    String s = text.trim().toLowerCase();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("empty duration");
    }
    if (s.startsWith("p")) {
      return Duration.parse(text.trim().toUpperCase());
    }
    long totalMillis = 0;
    int i = 0;
    boolean any = false;
    while (i < s.length()) {
      int start = i;
      while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) {
        i++;
      }
      if (start == i) {
        throw new IllegalArgumentException("invalid duration: " + text);
      }
      double value = Double.parseDouble(s.substring(start, i));
      int us = i;
      while (i < s.length() && !Character.isDigit(s.charAt(i))) {
        i++;
      }
      String unit = s.substring(us, i).trim();
      double millis;
      switch (unit) {
        case "ms":
          millis = value;
          break;
        case "s":
          millis = value * 1000d;
          break;
        case "m":
          millis = value * 60_000d;
          break;
        case "h":
          millis = value * 3_600_000d;
          break;
        case "d":
          millis = value * 86_400_000d;
          break;
        case "w":
          millis = value * 7 * 86_400_000d;
          break;
        case "ns":
        case "us":
          millis = value / 1000d;
          break;
        default:
          throw new IllegalArgumentException("invalid duration unit: " + unit + " in " + text);
      }
      totalMillis += (long) millis;
      any = true;
    }
    if (!any) {
      throw new IllegalArgumentException("invalid duration: " + text);
    }
    return Duration.ofMillis(totalMillis);
  }

  public static String format(Duration duration) {
    long seconds = duration.getSeconds();
    if (seconds % 86400 == 0 && seconds > 0) {
      return (seconds / 86400) + "d";
    }
    if (seconds % 3600 == 0 && seconds > 0) {
      return (seconds / 3600) + "h";
    }
    if (seconds % 60 == 0 && seconds > 0) {
      return (seconds / 60) + "m";
    }
    return seconds + "s";
  }
}
