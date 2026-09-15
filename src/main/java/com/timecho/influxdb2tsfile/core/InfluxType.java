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

import org.apache.tsfile.enums.TSDataType;

public enum InfluxType {
  FLOAT(TSDataType.DOUBLE),
  INTEGER(TSDataType.INT64),
  UNSIGNED(TSDataType.INT64),
  STRING(TSDataType.STRING),
  BOOLEAN(TSDataType.BOOLEAN);

  private final TSDataType tsDataType;

  InfluxType(TSDataType tsDataType) {
    this.tsDataType = tsDataType;
  }

  public TSDataType tsDataType() {
    return tsDataType;
  }

  public static InfluxType fromInfluxName(String name) {
    if (name == null) {
      return STRING;
    }
    switch (name.trim().toLowerCase()) {
      case "float":
      case "double":
        return FLOAT;
      case "integer":
      case "int":
      case "long":
        return INTEGER;
      case "unsigned":
      case "uinteger":
        return UNSIGNED;
      case "boolean":
      case "bool":
        return BOOLEAN;
      case "string":
        return STRING;
      default:
        return STRING;
    }
  }

  public static InfluxType fromName(String name) {
    return valueOf(name.trim().toUpperCase());
  }
}
