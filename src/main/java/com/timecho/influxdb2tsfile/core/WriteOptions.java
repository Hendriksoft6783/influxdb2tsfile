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

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tsfile.file.metadata.enums.CompressionType;

public class WriteOptions {

  public Path outDir = Paths.get(".");
  public String filePrefix = "";
  public long maxFileSizeBytes = 1024L * 1024L * 1024L;
  public long maxRowsPerFile = Long.MAX_VALUE;
  public int batchSize = 4096;
  public long maxBufferedRows = 200_000L;
  public CompressionType compression = null;
  public long memoryThresholdBytes = 64L * 1024L * 1024L;
  public boolean sanitizeNames = true;
  public String tablePrefix = "";

  public String partFileName(String unitId, int part) {
    StringBuilder sb = new StringBuilder();
    if (filePrefix != null && !filePrefix.isEmpty()) {
      sb.append(filePrefix).append('-');
    }
    sb.append(unitId);
    sb.append("-p").append(String.format("%04d", part)).append(".tsfile");
    return sb.toString();
  }
}
