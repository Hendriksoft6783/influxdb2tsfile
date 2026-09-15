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

import java.util.ArrayList;
import java.util.List;

public final class CsvLineParser {

  private CsvLineParser() {}

  public static List<String> parse(String line) {
    List<String> cells = new ArrayList<>();
    int length = line.length();
    int i = 0;
    while (i <= length) {
      if (i == length) {
        cells.add("");
        break;
      }
      StringBuilder sb = new StringBuilder();
      char c = line.charAt(i);
      if (c == '"') {
        i++;
        while (i < length) {
          char cur = line.charAt(i);
          if (cur == '\\' && i + 1 < length) {
            char next = line.charAt(i + 1);
            if (next == '"' || next == '\\') {
              sb.append(next);
              i += 2;
              continue;
            }
            sb.append(cur);
            i++;
            continue;
          }
          if (cur == '"') {
            if (i + 1 < length && line.charAt(i + 1) == '"') {
              sb.append('"');
              i += 2;
              continue;
            }
            i++;
            break;
          }
          sb.append(cur);
          i++;
        }
        while (i < length && line.charAt(i) != ',') {
          i++;
        }
        cells.add(sb.toString());
      } else {
        int start = i;
        while (i < length && line.charAt(i) != ',') {
          i++;
        }
        cells.add(line.substring(start, i));
      }
      if (i < length && line.charAt(i) == ',') {
        i++;
        if (i == length) {
          cells.add("");
          break;
        }
      } else if (i >= length) {
        break;
      }
    }
    return cells;
  }
}