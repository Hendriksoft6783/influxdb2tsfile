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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.timecho.influxdb2tsfile.Version;
import com.timecho.influxdb2tsfile.core.MigrationException;

public final class InfluxClient implements Closeable {

  private final String baseUrl;
  private final String token;
  private final String username;
  private final String password;
  private final Duration queryTimeout;
  private final HttpClient http;

  public InfluxClient(
      String url,
      String token,
      String username,
      String password,
      Duration connectTimeout,
      Duration queryTimeout) {
    this.baseUrl = normalize(url);
    this.token = emptyToNull(token);
    this.username = emptyToNull(username);
    this.password = emptyToNull(password);
    this.queryTimeout = queryTimeout;
    this.http =
        HttpClient.newBuilder()
            .connectTimeout(connectTimeout)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
  }

  private static String normalize(String url) {
    String value = url.trim();
    while (value.endsWith("/")) {
      value = value.substring(0, value.length() - 1);
    }
    if (!value.startsWith("http://") && !value.startsWith("https://")) {
      value = "http://" + value;
    }
    return value;
  }

  private static String emptyToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  public String baseUrl() {
    return baseUrl;
  }

  public ServerInfo ping() {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(baseUrl + "/ping"))
            .timeout(queryTimeout)
            .header("User-Agent", userAgent())
            .GET()
            .build();
    try {
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 400 && response.statusCode() != 401) {
        throw new MigrationException(
            "cannot reach InfluxDB at " + baseUrl + ": HTTP " + response.statusCode());
      }
      ServerInfo info = new ServerInfo();
      Optional<String> version = response.headers().firstValue("X-Influxdb-Version");
      Optional<String> build = response.headers().firstValue("X-Influxdb-Build");
      if (version.isPresent()) {
        info.version = version.get();
      }
      if (build.isPresent()) {
        info.build = build.get();
      }
      info.majorVersion = info.version.startsWith("1.") ? 1 : info.version.startsWith("2.") ? 2 : 0;
      return info;
    } catch (IOException e) {
      throw new MigrationException("cannot reach InfluxDB at " + baseUrl + ": " + e.getMessage(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MigrationException("interrupted while contacting " + baseUrl, e);
    }
  }

  public InputStream queryStream(String database, Map<String, String> params) throws IOException {
    Map<String, String> all = new LinkedHashMap<>();
    all.put("db", database);
    all.putAll(params);
    StringBuilder url = new StringBuilder(baseUrl).append("/query?");
    boolean first = true;
    for (Map.Entry<String, String> entry : all.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      if (!first) {
        url.append('&');
      }
      url.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
      first = false;
    }
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(url.toString()))
            .timeout(queryTimeout)
            .header("Accept", "application/csv")
            .header("Accept-Encoding", "identity")
            .header("User-Agent", userAgent());
    if (token != null) {
      builder.header("Authorization", "Token " + token);
    } else if (username != null) {
      String credentials = username + ":" + (password == null ? "" : password);
      builder.header(
          "Authorization",
          "Basic "
              + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
    }
    HttpResponse<InputStream> response;
    try {
      response = http.send(builder.GET().build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while querying InfluxDB", e);
    }
    if (response.statusCode() >= 400) {
      String body;
      try (InputStream in = response.body()) {
        body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      throw new MigrationException(
          "InfluxDB query failed with HTTP "
              + response.statusCode()
              + ": "
              + body.trim()
              + (response.statusCode() == 401
                  ? " (check --token or --username/--password)"
                  : "")
              + (response.statusCode() == 404
                  ? " (check that the database or bucket exists)"
                  : ""));
    }
    return response.body();
  }

  private static String userAgent() {
    return Version.TOOL_NAME + "/" + Version.VERSION;
  }

  private static String encode(String value) {
    try {
      return URLEncoder.encode(value, "UTF-8");
    } catch (Exception e) {
      return value;
    }
  }

  @Override
  public void close() {}
}
