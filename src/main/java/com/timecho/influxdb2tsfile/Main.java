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
package com.timecho.influxdb2tsfile;

import com.timecho.influxdb2tsfile.cli.DiscoverCommand;
import com.timecho.influxdb2tsfile.cli.InspectCommand;
import com.timecho.influxdb2tsfile.cli.Lp2TsFileCommand;
import com.timecho.influxdb2tsfile.cli.MigrateCommand;
import com.timecho.influxdb2tsfile.core.LogSetup;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "influxdb2tsfile",
    mixinStandardHelpOptions = true,
    version = "influxdb2tsfile 1.0.0 (Apache TsFile 2.4.0)",
    description = "Migrate InfluxDB data (1.x or 2.x) and line protocol files to Apache TsFile.",
    subcommands = {
      MigrateCommand.class,
      DiscoverCommand.class,
      Lp2TsFileCommand.class,
      InspectCommand.class
    })
public class Main implements Runnable {

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public static void main(String[] args) {
    LogSetup.configure(preScanLogLevel(args));
    CommandLine commandLine = new CommandLine(new Main());
    commandLine.setCaseInsensitiveEnumValuesAllowed(true);
    commandLine.setExecutionExceptionHandler(
        (exception, cmd, parseResult) -> {
          String message = exception.getMessage();
          System.err.println(
              "ERROR: " + (message == null || message.isEmpty() ? exception.toString() : message));
          if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "info").equals("debug")
              || System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "info").equals("trace")) {
            exception.printStackTrace();
          }
          return 1;
        });
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  private static String preScanLogLevel(String[] args) {
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("--log-level=")) {
        return arg.substring("--log-level=".length());
      }
      if (arg.equals("--log-level") && i + 1 < args.length) {
        return args[i + 1];
      }
    }
    return "info";
  }
}
