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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

class NamingTest {

  @Test
  void sanitizesSpecialCharacters() {
    assertEquals("usage_idle", Naming.sanitize("usage_idle"));
    assertEquals("cpu_usage", Naming.sanitize("cpu usage"));
    assertEquals("a_b_c", Naming.sanitize("a.b-c"));
    assertEquals("col", Naming.sanitize("///"));
    assertEquals("time_col", Naming.sanitize("time"));
    assertEquals("\u6d4b\u70b9", Naming.sanitize("\u6d4b\u70b9"));
  }

  @Test
  void lowerCasesNames() {
    assertEquals("usageidle", Naming.sanitize("UsageIdle"));
  }

  @Test
  void makesNamesUnique() {
    Set<String> used = new HashSet<>();
    String first = Naming.unique(Naming.sanitize("a b"), used);
    String second = Naming.unique(Naming.sanitize("a-b"), used);
    assertEquals("a_b", first);
    assertNotEquals(first, second);
    assertEquals("a_b_1", second);
  }
}
