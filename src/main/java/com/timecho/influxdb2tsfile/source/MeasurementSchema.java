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
package com.timecho.influxdb2tsfile.source;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.timecho.influxdb2tsfile.core.InfluxType;

public final class MeasurementSchema {

  private final String measurement;
  private final Map<String, InfluxType> fields = new TreeMap<>();
  private final Set<String> conflictingFields = new LinkedHashSet<>();
  private final Set<String> tagKeys = new TreeSet<>();
  private final Set<String> retentionPolicies = new TreeSet<>();
  private long seriesCount = -1;

  public MeasurementSchema(String measurement) {
    this.measurement = measurement;
  }

  public String measurement() {
    return measurement;
  }

  public void addField(String key, InfluxType type) {
    if (key == null || key.isEmpty()) {
      return;
    }
    InfluxType existing = fields.get(key);
    if (existing == null) {
      fields.put(key, type);
    } else if (existing != type) {
      conflictingFields.add(key);
    }
  }

  public void addTagKey(String key) {
    if (key != null && !key.isEmpty()) {
      tagKeys.add(key);
    }
  }

  public void addRetentionPolicy(String rp) {
    if (rp != null && !rp.isEmpty()) {
      retentionPolicies.add(rp);
    }
  }

  public Map<String, InfluxType> fields() {
    return Collections.unmodifiableMap(new LinkedHashMap<>(fields));
  }

  public Set<String> tagKeys() {
    return Collections.unmodifiableSet(tagKeys);
  }

  public Set<String> conflicts() {
    return Collections.unmodifiableSet(conflictingFields);
  }

  public Set<String> retentionPolicies() {
    return Collections.unmodifiableSet(retentionPolicies);
  }

  public long seriesCount() {
    return seriesCount;
  }

  public void setSeriesCount(long seriesCount) {
    this.seriesCount = seriesCount;
  }

  @Override
  public String toString() {
    return "MeasurementSchema{"
        + measurement
        + ", fields="
        + fields
        + ", tags="
        + tagKeys
        + (conflictingFields.isEmpty() ? "" : ", conflicts=" + conflictingFields)
        + "}";
  }
}
