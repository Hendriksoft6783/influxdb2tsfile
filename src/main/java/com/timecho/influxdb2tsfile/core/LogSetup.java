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

public final class LogSetup {

  private LogSetup() {}

  public static void configure(String level) {
    setIfAbsent("org.slf4j.simpleLogger.defaultLogLevel", level);
    setIfAbsent("org.slf4j.simpleLogger.showDateTime", "true");
    setIfAbsent("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss");
    setIfAbsent("org.slf4j.simpleLogger.showThreadName", "false");
    setIfAbsent("org.slf4j.simpleLogger.showLogName", "false");
    setIfAbsent("org.slf4j.simpleLogger.showShortLogName", "true");
    setIfAbsent("org.slf4j.simpleLogger.log.org.apache.tsfile", "warn");
  }

  private static void setIfAbsent(String key, String value) {
    if (System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }
}
