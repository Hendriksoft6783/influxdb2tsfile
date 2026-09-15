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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.timecho.influxdb2tsfile.core.MigrationException;

import org.junit.jupiter.api.Test;

class CsvParserTest {

  @Test
  void parsesQuotedAndEscapedCells() {
    List<String> cells = CsvLineParser.parse("cpu,\"host=a,region=cn\",1704067200000000000,1.5,\"a\\\"b\"");
    assertEquals(5, cells.size());
    assertEquals("cpu", cells.get(0));
    assertEquals("host=a,region=cn", cells.get(1));
    assertEquals("1704067200000000000", cells.get(2));
    assertEquals("a\"b", cells.get(4));
  }

  @Test
  void parsesEmptyCells() {
    List<String> cells = CsvLineParser.parse("cpu,,1706,1.5,");
    assertEquals(5, cells.size());
    assertEquals("", cells.get(1));
    assertEquals("", cells.get(4));
  }

  @Test
  void infersValueTypesFromSyntax() {
    assertEquals(1.5d, CsvValueParser.parseBySyntax("1.5"));
    assertEquals(7L, CsvValueParser.parseBySyntax("7i"));
    assertEquals(1704067200123456789L, CsvValueParser.parseBySyntax("1704067200123456789"));
    assertEquals(Boolean.TRUE, CsvValueParser.parseBySyntax("true"));
    assertEquals(Boolean.FALSE, CsvValueParser.parseBySyntax("F"));
    assertEquals("text", CsvValueParser.parseBySyntax("text"));
    assertNull(CsvValueParser.parseBySyntax(""));
    assertEquals("string", CsvValueParser.parse("string", "string"));
    assertEquals(3L, CsvValueParser.parse("3", "long"));
  }

  @Test
  void parsesAnnotatedFluxCsv() throws Exception {
    String csv =
        "#datatype,string,long,dateTime:RFC3339,string,string,double\n"
            + "#group,false,false,false,true,true,false\n"
            + "#default,_result,,,,,\n"
            + ",result,table,_time,_measurement,host,usage\n"
            + ",_result,0,2024-01-01T00:00:00Z,cpu,host-a,1.5\n"
            + ",_result,0,2024-01-01T00:01:00Z,cpu,host-a,2.5\n"
            + "\n"
            + ",result,table,_time,_measurement,host,usage\n"
            + ",_result,1,2024-01-01T00:00:00Z,cpu,host-b,3.5\n";
    List<String> headers = new ArrayList<>();
    List<String> rows = new ArrayList<>();
    InfluxCsvParser.parse(
        new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)),
        new InfluxCsvParser.Handler() {
          @Override
          public void header(int headerId, List<String> columns, List<String> datatypes) {
            headers.add(headerId + ":" + String.join(",", columns) + "|" + String.join(",", datatypes));
          }

          @Override
          public void row(int headerId, List<String> values) {
            rows.add(headerId + ":" + String.join(",", values));
          }
        });
    assertEquals(2, headers.size());
    assertEquals(3, rows.size());
    assertTrue(headers.get(0).contains("_time"));
    assertTrue(rows.get(0).contains("2024-01-01T00:00:00Z"));
  }

  @Test
  void detectsStreamingErrors() {
    String csv = "name,tags,time,value\ncpu,,1,1.5\n{\"error\":\"partial write\"}\n";
    assertThrows(
        MigrationException.class,
        () ->
            InfluxCsvParser.parse(
                new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)),
                new InfluxCsvParser.Handler() {
                  @Override
                  public void header(int headerId, List<String> columns, List<String> datatypes) {}

                  @Override
                  public void row(int headerId, List<String> values) {}
                }));
  }
}