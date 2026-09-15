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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class Times {

  public static final long MIN_NANOS = -9223372036854775806L;

  private static final DateTimeFormatter DISPLAY =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  private Times() {}

  public static long parseToNanos(String text) {
    String value = text.trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException("empty time value");
    }
    if (value.equalsIgnoreCase("now")) {
      return System.currentTimeMillis() * 1_000_000L;
    }
    if (value.startsWith("-") || value.startsWith("+")) {
      Duration duration = DurationFormat.parse(value.substring(1));
      long delta = duration.toNanos();
      return value.startsWith("-")
          ? System.currentTimeMillis() * 1_000_000L - delta
          : System.currentTimeMillis() * 1_000_000L + delta;
    }
    if (value.matches("-?\\d+")) {
      return epochToNanos(Long.parseLong(value));
    }
    try {
      return Instant.parse(value).getEpochSecond() * 1_000_000_000L + Instant.parse(value).getNano();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC).toEpochMilli() * 1_000_000L;
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDate.parse(value).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
          * 1_000_000L;
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    throw new IllegalArgumentException(
        "unsupported time value: "
            + text
            + " (use RFC3339, epoch seconds/millis/micros/nanos, now or -7d)");
  }

  private static long epochToNanos(long value) {
    int digits = String.valueOf(Math.abs(value)).length();
    if (digits <= 11) {
      return value * 1_000_000_000L;
    }
    if (digits <= 14) {
      return value * 1_000_000L;
    }
    if (digits <= 17) {
      return value * 1_000L;
    }
    return value;
  }

  public static String formatExact(long nanos) {
    return DateTimeFormatter.ISO_INSTANT.format(
        Instant.ofEpochSecond(Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L)));
  }

  public static String format(long nanos) {
    long seconds = Math.floorDiv(nanos, 1_000_000_000L);
    long rest = Math.floorMod(nanos, 1_000_000_000L);
    return DISPLAY.format(Instant.ofEpochSecond(seconds, rest));
  }

  public static String toRfc3339(long nanos) {
    long seconds = Math.floorDiv(nanos, 1_000_000_000L);
    long rest = Math.floorMod(nanos, 1_000_000_000L);
    return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(seconds, rest));
  }
}