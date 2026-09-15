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
package com.timecho.influxdb2tsfile.support;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class MockInfluxServer implements Closeable {

  public interface QueryHandler {
    String respond(String query, Map<String, String> params);
  }

  private final HttpServer server;
  private volatile QueryHandler handler;
  private final AtomicInteger queryCount = new AtomicInteger();
  private volatile int failuresRemaining;

  public MockInfluxServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  public void setHandler(QueryHandler handler) {
    this.handler = handler;
  }

  public void failNextDataQueries(int count) {
    failuresRemaining = count;
  }

  public int queryCount() {
    return queryCount.get();
  }

  public String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/ping")) {
        exchange.getResponseHeaders().add("X-Influxdb-Version", "1.8.10");
        exchange.getResponseHeaders().add("X-Influxdb-Build", "OSS");
        exchange.sendResponseHeaders(204, -1);
        return;
      }
      Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
      String query = params.get("q");
      queryCount.incrementAndGet();
      boolean isDataQuery = query != null && query.contains("GROUP BY");
      if (isDataQuery && failuresRemaining > 0) {
        failuresRemaining--;
        respondError(exchange, 500, "{\"error\":\"injected failure\"}");
        return;
      }
      String csv = handler == null ? null : handler.respond(query, params);
      if (csv == null) {
        respondError(exchange, 400, "{\"error\":\"unsupported query: " + query + "\"}");
        return;
      }
      byte[] body = csv.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "text/csv; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    } finally {
      exchange.close();
    }
  }

  private static void respondError(HttpExchange exchange, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(code, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static Map<String, String> parseQuery(String rawQuery) {
    Map<String, String> params = new LinkedHashMap<>();
    if (rawQuery == null) {
      return params;
    }
    for (String pair : rawQuery.split("&")) {
      int eq = pair.indexOf('=');
      if (eq < 0) {
        continue;
      }
      try {
        params.put(
            URLDecoder.decode(pair.substring(0, eq), "UTF-8"),
            URLDecoder.decode(pair.substring(eq + 1), "UTF-8"));
      } catch (Exception ignored) {
        // ignore malformed parameter
      }
    }
    return params;
  }

  @Override
  public void close() {
    server.stop(0);
  }
}