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

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public final class Naming {

  private Naming() {}

  public static String sanitize(String raw) {
    if (raw == null || raw.isEmpty()) {
      return "col";
    }
    String lower = raw.toLowerCase(Locale.ROOT);
    StringBuilder sb = new StringBuilder(lower.length());
    boolean lastUnderscore = false;
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      if (isAllowed(c)) {
        sb.append(c);
        lastUnderscore = false;
      } else if (!lastUnderscore) {
        sb.append('_');
        lastUnderscore = true;
      }
    }
    String result = sb.toString();
    while (result.startsWith("_")) {
      result = result.substring(1);
    }
    while (result.endsWith("_")) {
      result = result.substring(0, result.length() - 1);
    }
    if (result.isEmpty()) {
      result = "col";
    }
    if (result.equals("time")) {
      result = "time_col";
    }
    return result;
  }

  private static boolean isAllowed(char c) {
    if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
      return true;
    }
    return Character.UnicodeScript.of(c) == Character.UnicodeScript.HAN;
  }

  public static String unique(String candidate, Set<String> used) {
    if (!used.contains(candidate)) {
      used.add(candidate);
      return candidate;
    }
    int suffix = 1;
    while (true) {
      String next = candidate + "_" + suffix;
      if (!used.contains(next)) {
        used.add(next);
        return next;
      }
      suffix++;
    }
  }

  public static Set<String> newSet() {
    return new HashSet<>();
  }
}
