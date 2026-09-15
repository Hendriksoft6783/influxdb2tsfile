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

public enum TimePrecision {
  S(1_000_000_000L),
  MS(1_000_000L),
  US(1_000L),
  NS(1L);

  private final long nanosPerUnit;

  TimePrecision(long nanosPerUnit) {
    this.nanosPerUnit = nanosPerUnit;
  }

  public long toNanos(long value) {
    return value * nanosPerUnit;
  }

  public long nanosPerUnit() {
    return nanosPerUnit;
  }
}
