# influxdb2tsfile

[![CI](https://github.com/TimechoLab/influxdb2tsfile/actions/workflows/ci.yml/badge.svg)](https://github.com/TimechoLab/influxdb2tsfile/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](#requirements)

**English** · [中文](README.zh.md)

A command-line tool that migrates InfluxDB data to [Apache TsFile](https://github.com/apache/tsfile).

> **Why it exists**: Alibaba Cloud InfluxDB® is being retired — no new purchases after 2025-10-23,
> no renewals or scaling after 2026-04-23, and the service is terminated on 2026-10-23.
> This tool moves the historical data out of InfluxDB into columnar TsFile files, so it can keep
> living in [TimechoDB](https://www.timecho.com/), Apache IoTDB, or any TsFile reader.

## Download

Prebuilt packages are on the [Releases](https://github.com/TimechoLab/influxdb2tsfile/releases) page:

| File | What it is |
| --- | --- |
| `influxdb2tsfile-<version>-bin.tar.gz` | Runnable package: `bin/influxdb2tsfile` launcher + fat-jar in `lib/` + READMEs + LICENSE |
| `influxdb2tsfile-<version>.jar` | The shaded fat-jar alone |

```bash
tar xzf influxdb2tsfile-1.0.0-bin.tar.gz && cd influxdb2tsfile-1.0.0
export JAVA_HOME=/path/to/jdk-17      # JDK 17+ is required
bin/influxdb2tsfile --help
```

## Features

- **InfluxDB 1.x** (InfluxQL, username/password) and **InfluxDB 2.x** (InfluxQL compatibility API, API token) — same commands and options for both
- **Offline migration** from line protocol files or stdin (`lp2tsfile`), for instances that are already gone
- **Automatic schema mapping**: measurement → table, tag → TAG column, field → FIELD column, with type mapping (float→DOUBLE, integer/unsigned→INT64, boolean→BOOLEAN, string→STRING)
- **Streaming read + sharded write**: time slicing (`--time-slice`), rolling by size/rows (`--max-file-size` / `--max-rows-per-file`), predictable memory usage
- **Parallel export** (`--parallel`): every work unit writes its own files, no shared state
- **Resumable** (`--resume`): failed units are retried, completed units are skipped, no duplicated rows
- **Built-in verification**: `--verify` and `inspect --manifest` read the written TsFile files back and compare row counts against the manifest
- **`manifest.json`** per run: files, row counts, time ranges and column mapping per unit — handy for auditing and for downstream tooling
- **Single fat-jar**, no external dependency besides the JRE

## Requirements

| Item | Requirement |
| --- | --- |
| JDK | **17 or newer** for build and runtime. TsFile 2.4.0 is compiled to Java 17 bytecode; older JDKs fail with `UnsupportedClassVersionError` / `class file version 61.0` |
| Maven | 3.6 or newer (build only) |
| Apache TsFile | 2.4.0 (bundled in the fat-jar) |

The launcher script `bin/influxdb2tsfile` checks the JDK version up front and prints a clear message instead of an obscure `UnsupportedClassVersionError`.

## Build

```bash
mvn -DskipTests package      # produces target/influxdb2tsfile-1.0.0.jar (dependencies included)
mvn test                     # unit + integration tests (uses an in-process mock InfluxDB, no real server needed)
```

## Quick start

### 1. Inspect the schema and the TsFile mapping

```bash
java -jar target/influxdb2tsfile-1.0.0.jar discover \
  --url http://127.0.0.1:8086 --database telegraf --counts
```

It prints every measurement with its tags, fields, field types and estimated point count, plus the
table/column names that will be produced.

### 2. Migrate data

```bash
java -jar target/influxdb2tsfile-1.0.0.jar migrate \
  --url http://127.0.0.1:8086 --database telegraf \
  --start -90d --end now \
  --time-slice 1d --parallel 4 \
  -o /data/tsfile-out --verify
```

InfluxDB 2.x only differs by the credentials:

```bash
java -jar target/influxdb2tsfile-1.0.0.jar migrate \
  --url http://127.0.0.1:8086 --database telegraf --token <api-token> \
  --time-slice 1d --parallel 4 -o /data/tsfile-out --verify
```

### 3. Offline migration from line protocol

```bash
java -jar target/influxdb2tsfile-1.0.0.jar lp2tsfile /backup/export.lp -o /data/tsfile-out
cat /backup/export.lp | java -jar target/influxdb2tsfile-1.0.0.jar lp2tsfile - -o /data/tsfile-out
```

### 4. Inspect and verify the result

```bash
java -jar target/influxdb2tsfile-1.0.0.jar inspect /data/tsfile-out --rows 3
java -jar target/influxdb2tsfile-1.0.0.jar inspect /data/tsfile-out --manifest /data/tsfile-out/manifest.json
```

## Data mapping

| InfluxDB | Apache TsFile (table model) |
| --- | --- |
| database / bucket | output directory (use `--table-prefix` to keep the database name in the table names) |
| measurement | table name |
| tag key / value | TAG column (STRING); the tag values of a row form the device id (series) |
| field key | FIELD column; float→DOUBLE, integer/unsigned→INT64, boolean→BOOLEAN, string→STRING |
| timestamp | row timestamp (nanoseconds, the native TsFile time column) |
| field missing in a row | null (bitmap marked, no data space wasted) |

## Commands

| Command | Purpose | Useful options |
| --- | --- | --- |
| `migrate` | read from InfluxDB and write TsFile files | `--measurements`, `--measurement-regex`, `--start/--end`, `--time-slice`, `--parallel`, `--resume`, `--verify`, `--dry-run` |
| `discover` | print measurements, tags, fields and the TsFile mapping | `--counts`, `--json`, `--report <file>` |
| `lp2tsfile` | convert line protocol files or stdin to TsFile | `--precision`, `--default-time`, `--skip-invalid` |
| `inspect` | show tables, columns, row counts, devices, time ranges, sample rows | `--rows N`, `--json`, `--manifest <file>` |

Run any command with `--help` for the full option list. Common write options:
`--max-file-size`, `--batch-size`, `--compression` (UNCOMPRESSED/SNAPPY/GZIP/LZ4/ZSTD/LZMA2),
`--no-sanitize`, `--exclude-tags`, `--exclude-fields`, `--field-type`, `--type-conflict`.

## Reliability

- A run is split into **work units** (one measurement × one time slice). Each unit owns its files, so units can run in
  parallel, be retried on their own, and never mix data.
- Re-running the same command is **idempotent**: the files of a unit are removed before it is rewritten.
- `--resume` loads the manifest of a previous run, re-validates the recorded output files (existence and size),
  and only redoes what is missing, failed, or stale.
- `--verify` (or `inspect --manifest` later) compares the row count of every table in the written files
  with the manifest; a mismatch exits with code 1.

## Output layout

```
/data/tsfile-out/
├── cpu_00000-p0000.tsfile      # measurement cpu, time slice 0, part 0
├── cpu_00000-p0001.tsfile      #   (rolled because the slice exceeded --max-file-size)
├── cpu_00001-p0000.tsfile      # time slice 1
├── mem_00000-p0000.tsfile
└── manifest.json               # per-unit files, rows, devices, time ranges, column mapping
```

## Reading the generated TsFile

```xml
<dependency>
  <groupId>org.apache.tsfile</groupId>
  <artifactId>tsfile</artifactId>
  <version>2.4.0</version>
</dependency>
```

```java
try (ITsFileReader reader = new TsFileReaderBuilder().file(new File("cpu_00000-p0000.tsfile")).build();
     ResultSet rs = reader.query("cpu", List.of("host", "usage_idle"), Long.MIN_VALUE, Long.MAX_VALUE)) {
  while (rs.next()) {
    long timeNanos = rs.getLong("Time");
    String host = rs.isNull("host") ? null : rs.getString("host");
    Double usage = rs.isNull("usage_idle") ? null : rs.getDouble("usage_idle");
    System.out.println(timeNanos + " " + host + " " + usage);
  }
}
```

## Documentation

- [User guide (Chinese)](docs/用户手册.md) — migration playbook for the Alibaba Cloud retirement, full option reference, FAQ, limitations
- [Design notes (Chinese)](docs/DESIGN.md) — architecture, mapping specification, work units, manifest, error handling

## Limitations

- Only time-series data is migrated. InfluxDB metadata (users, dashboards, continuous queries, subscriptions, tasks) is out of scope.
- One measurement maps to one table; migrating multiple databases/buckets into the same directory needs `--table-prefix`.
- Only the default retention policy is read by default; migrate other RPs explicitly with `--rp`.
- A field that has multiple data types in InfluxDB must be migrated with `--type-conflict text` (stored as STRING) or fixed at the source.
- TsFile table-model names are case-insensitive; uppercase keys are lower-cased (the original names are recorded in the manifest).
- Command line only — there is no web UI, by design.

## License

Apache License 2.0 — see [LICENSE](LICENSE).