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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class SizeFormatTest {

  @Test
  void parsesSizes() {
    assertEquals(1024L, SizeFormat.parseBytes("1KB"));
    assertEquals(1024L * 1024L, SizeFormat.parseBytes("1m"));
    assertEquals(512L * 1024L * 1024L, SizeFormat.parseBytes("512MB"));
    assertEquals(3L * 1024L * 1024L * 1024L, SizeFormat.parseBytes("3G"));
    assertEquals(100L, SizeFormat.parseBytes("100"));
    assertThrows(IllegalArgumentException.class, () -> SizeFormat.parseBytes("abc"));
  }

  @Test
  void parsesDurations() {
    assertEquals(Duration.ofDays(7), DurationFormat.parse("7d"));
    assertEquals(Duration.ofHours(6), DurationFormat.parse("6h"));
    assertEquals(Duration.ofMinutes(90), DurationFormat.parse("1h30m"));
    assertEquals(Duration.ofSeconds(30), DurationFormat.parse("30s"));
    assertThrows(IllegalArgumentException.class, () -> DurationFormat.parse("5x"));
  }

  @Test
  void parsesTimes() {
    assertEquals(1704067200000000000L, Times.parseToNanos("2024-01-01T00:00:00Z"));
    assertEquals(1704067200000000000L, Times.parseToNanos("1704067200000000000"));
    assertEquals(1704067200000000000L, Times.parseToNanos("1704067200000"));
    assertEquals(1704067200000000000L, Times.parseToNanos("1704067200"));
    assertEquals(
        "2024-01-01T00:00:00.123456789Z",
        Times.formatExact(Times.parseToNanos("2024-01-01T00:00:00.123456789Z")));
  }
}