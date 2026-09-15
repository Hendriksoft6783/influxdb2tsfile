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
package com.timecho.influxdb2tsfile.source.lp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.timecho.influxdb2tsfile.core.MigrationException;
import com.timecho.influxdb2tsfile.core.TimePrecision;

import org.junit.jupiter.api.Test;

class LineProtocolParserTest {

  private static final class Captured {
    String measurement;
    Map<String, String> tags;
    Map<String, Object> fields;
    long time;
  }

  private List<Captured> parse(String... lines) {
    LineProtocolParser parser = new LineProtocolParser(TimePrecision.NS, null);
    List<Captured> result = new ArrayList<>();
    for (String line : lines) {
      parser.parseLine(
          line,
          (measurement, tags, fields, timeNanos) -> {
            Captured captured = new Captured();
            captured.measurement = measurement;
            captured.tags = tags;
            captured.fields = fields;
            captured.time = timeNanos;
            result.add(captured);
          });
    }
    return result;
  }

  @Test
  void parsesTypes() {
    List<Captured> captured =
        parse("weather,location=us-midwest temp=82.5,count=7i,online=true,desc=\"too warm\" 1465839830100400200");
    assertEquals(1, captured.size());
    Captured point = captured.get(0);
    assertEquals("weather", point.measurement);
    assertEquals("us-midwest", point.tags.get("location"));
    assertEquals(82.5d, (Double) point.fields.get("temp"), 0.0001d);
    assertEquals(7L, point.fields.get("count"));
    assertEquals(Boolean.TRUE, point.fields.get("online"));
    assertEquals("too warm", point.fields.get("desc"));
    assertEquals(1465839830100400200L, point.time);
  }

  @Test
  void parsesUnsignedInteger() {
    List<Captured> captured = parse("m,u=1 v=18446744073709551615u 1");
    assertEquals(Long.parseUnsignedLong("18446744073709551615"), captured.get(0).fields.get("v"));
    assertThrows(MigrationException.class, () -> parse("m v=18446744073709551616u 1"));
  }

  @Test
  void handlesEscapedCharacters() {
    List<Captured> captured = parse("my\\ measurement,tag\\ key=tag\\,value field\\ key=\"value with \\\"quote\\\"\" 1000");
    assertEquals("my measurement", captured.get(0).measurement);
    assertEquals("tag,value", captured.get(0).tags.get("tag key"));
    assertEquals("value with \"quote\"", captured.get(0).fields.get("field key"));
  }

  @Test
  void skipsCommentsAndEmptyLines() {
    List<Captured> captured = parse("# comment", "", "   ", "m v=1i 1");
    assertEquals(1, captured.size());
  }

  @Test
  void rejectsMissingFieldSet() {
    assertThrows(MigrationException.class, () -> parse("m,tag=value 1704067200000000000"));
    assertThrows(MigrationException.class, () -> parse("m v=abc 1704067200000000000"));
  }

  @Test
  void requiresDefaultTimeWhenMissing() {
    assertThrows(MigrationException.class, () -> parse("m v=1i"));
    LineProtocolParser parser = new LineProtocolParser(TimePrecision.NS, 12345L);
    assertTrue(parser.parseLine("m v=1i", (measurement, tags, fields, timeNanos) -> assertEquals(12345L, timeNanos)));
    assertFalse(parser.parseLine("", (measurement, tags, fields, timeNanos) -> {}));
  }

  @Test
  void scalesTimestampPrecision() {
    LineProtocolParser parser = new LineProtocolParser(TimePrecision.S, null);
    parser.parseLine("m v=1i 1704067200", (measurement, tags, fields, timeNanos) -> assertEquals(1704067200000000000L, timeNanos));
  }
}
