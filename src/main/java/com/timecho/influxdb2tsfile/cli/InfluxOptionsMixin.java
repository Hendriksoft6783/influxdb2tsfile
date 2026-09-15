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
package com.timecho.influxdb2tsfile.cli;

import com.timecho.influxdb2tsfile.core.DurationFormat;
import com.timecho.influxdb2tsfile.source.influx.InfluxClient;

import picocli.CommandLine.Option;

public class InfluxOptionsMixin {

  @Option(
      names = {"-u", "--url"},
      description = "InfluxDB address, e.g. http://127.0.0.1:8086 (env: INFLUX_URL)",
      defaultValue = "${env:INFLUX_URL:-}")
  public String url;

  @Option(
      names = {"-d", "--database", "--bucket"},
      description = "InfluxDB database (1.x) or bucket (2.x) (env: INFLUX_DATABASE, INFLUX_BUCKET)",
      defaultValue = "${env:INFLUX_DATABASE:-}${env:INFLUX_BUCKET:-}")
  public String database;

  @Option(
      names = "--token",
      description = "API token of InfluxDB 2.x (or 1.x with authentication enabled) (env: INFLUX_TOKEN)",
      defaultValue = "${env:INFLUX_TOKEN:-}")
  public String token;

  @Option(
      names = {"--username", "-U"},
      description = "user name of InfluxDB 1.x (env: INFLUX_USERNAME)",
      defaultValue = "${env:INFLUX_USERNAME:-}")
  public String username;

  @Option(
      names = {"--password", "-P"},
      description = "password of InfluxDB 1.x (env: INFLUX_PASSWORD)",
      defaultValue = "${env:INFLUX_PASSWORD:-}")
  public String password;

  @Option(names = "--org", description = "organisation name of InfluxDB 2.x (env: INFLUX_ORG)", defaultValue = "${env:INFLUX_ORG:-}")
  public String org;

  @Option(
      names = {"--rp", "--retention-policy"},
      description = "retention policy (1.x), default: the default retention policy of the database")
  public String retentionPolicy;

  @Option(names = "--server-version", description = "expected server version: auto, 1 or 2 (default: ${DEFAULT-VALUE})", defaultValue = "auto")
  public String serverVersion;

  @Option(names = "--connect-timeout", description = "connection timeout (default: ${DEFAULT-VALUE})", defaultValue = "10s")
  public String connectTimeout;

  @Option(names = "--query-timeout", description = "timeout while waiting for the response header (default: ${DEFAULT-VALUE})", defaultValue = "300s")
  public String queryTimeout;

  @Option(names = "--chunk-size", description = "points per streamed chunk (default: ${DEFAULT-VALUE})", defaultValue = "10000")
  public int chunkSize;

  public InfluxClient toClient() {
    if (url == null || url.trim().isEmpty()) {
      throw new IllegalArgumentException("InfluxDB url is required, use --url or the INFLUX_URL environment variable");
    }
    if (database == null || database.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "database (1.x) or bucket (2.x) is required, use --database/--bucket or INFLUX_DATABASE/INFLUX_BUCKET");
    }
    return new InfluxClient(
        url,
        token,
        username,
        password,
        DurationFormat.parse(connectTimeout == null || connectTimeout.isEmpty() ? "10s" : connectTimeout),
        DurationFormat.parse(queryTimeout == null || queryTimeout.isEmpty() ? "300s" : queryTimeout));
  }
}