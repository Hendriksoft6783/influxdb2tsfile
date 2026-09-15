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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.timecho.influxdb2tsfile.source.MeasurementSchema;

import org.junit.jupiter.api.Test;

class SchemaPlannerTest {

  private MeasurementSchema schema() {
    MeasurementSchema schema = new MeasurementSchema("cpu usage");
    schema.addTagKey("host name");
    schema.addTagKey("region");
    schema.addField("usage idle", InfluxType.FLOAT);
    schema.addField("cores", InfluxType.INTEGER);
    return schema;
  }

  @Test
  void buildsSpec() {
    TableSpec spec = SchemaPlanner.plan(schema(), new SchemaOptions());
    assertEquals("cpu_usage", spec.tableName());
    assertEquals(2, spec.tagCount());
    assertEquals(2, spec.fieldCount());
    assertEquals("host_name", spec.tagColumns().get(0));
    assertEquals("cores", spec.fieldColumns().get(0));
    assertEquals("usage_idle", spec.fieldColumns().get(1));
    assertEquals(org.apache.tsfile.enums.TSDataType.INT64, spec.fieldTypes().get(0));
    assertEquals(org.apache.tsfile.enums.TSDataType.DOUBLE, spec.fieldTypes().get(1));
    assertEquals(4, spec.columnCount());
    assertEquals(
        java.util.Arrays.asList(
            org.apache.tsfile.enums.ColumnCategory.TAG,
            org.apache.tsfile.enums.ColumnCategory.TAG,
            org.apache.tsfile.enums.ColumnCategory.FIELD,
            org.apache.tsfile.enums.ColumnCategory.FIELD),
        spec.columnCategories());
  }

  @Test
  void appliesTablePrefixAndTagFilter() {
    SchemaOptions options = new SchemaOptions();
    options.tablePrefix = "telegraf";
    options.excludeTagKeys.add("region");
    TableSpec spec = SchemaPlanner.plan(schema(), options);
    assertEquals("telegraf_cpu_usage", spec.tableName());
    assertEquals(1, spec.tagCount());
  }

  @Test
  void renamesFieldCollidingWithTag() {
    MeasurementSchema schema = new MeasurementSchema("m");
    schema.addTagKey("value");
    schema.addField("value", InfluxType.FLOAT);
    TableSpec spec = SchemaPlanner.plan(schema, new SchemaOptions());
    assertEquals("value", spec.tagColumns().get(0));
    assertEquals("value_1", spec.fieldColumns().get(0));
  }

  @Test
  void failsOnTypeConflictByDefault() {
    MeasurementSchema schema = new MeasurementSchema("m");
    schema.addField("v", InfluxType.FLOAT);
    schema.addField("v", InfluxType.STRING);
    assertThrows(SchemaConflictException.class, () -> SchemaPlanner.plan(schema, new SchemaOptions()));
    SchemaOptions options = new SchemaOptions();
    options.typeConflictPolicy = SchemaOptions.TypeConflictPolicy.TEXT;
    TableSpec spec = SchemaPlanner.plan(schema, options);
    assertEquals(org.apache.tsfile.enums.TSDataType.STRING, spec.fieldTypes().get(0));
  }

  @Test
  void convertsValuesToColumnTypes() {
    MeasurementSchema schema = new MeasurementSchema("m");
    schema.addField("f", InfluxType.FLOAT);
    schema.addField("i", InfluxType.INTEGER);
    schema.addField("s", InfluxType.STRING);
    TableSpec spec = SchemaPlanner.plan(schema, new SchemaOptions());
    assertEquals(1.5d, spec.coerce(0, 1.5d));
    assertEquals(2.0d, spec.coerce(0, 2L));
    assertEquals(3L, spec.coerce(1, 3L));
    assertEquals(4L, spec.coerce(1, 4));
    assertEquals("5", spec.coerce(2, 5L));
    assertTrue(spec.sourceKeyByColumn().containsKey("f"));
  }
}