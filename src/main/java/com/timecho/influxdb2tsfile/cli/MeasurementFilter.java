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
package com.timecho.influxdb2tsfile.cli;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class MeasurementFilter {

  private MeasurementFilter() {}

  public static List<String> select(
      List<String> available, List<String> names, String regex, boolean ignoreCase) {
    List<String> selected = new ArrayList<>();
    Set<String> nameSet = names == null ? new LinkedHashSet<>() : new LinkedHashSet<>(names);
    Pattern pattern = null;
    if (regex != null && !regex.isEmpty()) {
      pattern = Pattern.compile(regex, ignoreCase ? Pattern.CASE_INSENSITIVE : 0);
    }
    for (String measurement : available) {
      if (!nameSet.isEmpty()) {
        boolean matched = false;
        for (String name : nameSet) {
          if (ignoreCase ? name.equalsIgnoreCase(measurement) : name.equals(measurement)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          continue;
        }
      }
      if (pattern != null && !pattern.matcher(measurement).find()) {
        continue;
      }
      selected.add(measurement);
    }
    return selected;
  }
}
