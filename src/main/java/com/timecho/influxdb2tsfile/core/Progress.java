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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Progress {

  private static final Logger LOG = LoggerFactory.getLogger(Progress.class);

  private final AtomicLong rows = new AtomicLong();
  private final AtomicLong files = new AtomicLong();
  private final AtomicLong bytes = new AtomicLong();
  private final long startedAt = System.currentTimeMillis();
  private volatile long lastReportAt = startedAt;

  public void addRows(long count) {
    rows.addAndGet(count);
  }

  public void addFile(long size) {
    files.incrementAndGet();
    bytes.addAndGet(size);
  }

  public long rows() {
    return rows.get();
  }

  public long files() {
    return files.get();
  }

  public long bytes() {
    return bytes.get();
  }

  public void report() {
    long now = System.currentTimeMillis();
    if (now - lastReportAt < 5000) {
      return;
    }
    lastReportAt = now;
    double seconds = Math.max(0.001d, (now - startedAt) / 1000d);
    long rowCount = rows.get();
    LOG.info(
        "progress: rows={} ({} rows/s), files={}, bytes={}",
        rowCount,
        (long) (rowCount / seconds),
        files.get(),
        SizeFormat.format(bytes.get()));
  }

  public String summary() {
    long elapsed = Math.max(1L, System.currentTimeMillis() - startedAt) / 1000L;
    return String.format(
        "rows=%d, files=%d, size=%s, elapsed=%ds, throughput=%d rows/s",
        rows.get(), files.get(), SizeFormat.format(bytes.get()), elapsed, rows.get() / Math.max(1, elapsed));
  }
}
