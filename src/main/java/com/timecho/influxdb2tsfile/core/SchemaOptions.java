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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SchemaOptions {

  public enum TypeConflictPolicy {
    FAIL,
    TEXT
  }

  public boolean sanitizeNames = true;
  public String tablePrefix = "";
  public boolean includeTags = true;
  public Set<String> includeTagKeys = null;
  public Set<String> excludeTagKeys = new LinkedHashSet<>();
  public Set<String> excludeFieldKeys = new LinkedHashSet<>();
  public Map<String, InfluxType> fieldTypeOverrides = new HashMap<>();
  public TypeConflictPolicy typeConflictPolicy = TypeConflictPolicy.FAIL;
}
