# influxdb2tsfile

[English](README.md) · **中文**

把 InfluxDB 数据迁移到 [Apache TsFile](https://github.com/apache/tsfile) 的命令行工具。

> 背景：阿里云 InfluxDB® 版将于 2026-10-23 正式退市（2025-10-23 停止新购、2026-04-23 停止续费扩容）。
> 本工具用于在退市前把 InfluxDB 中的历史数据完整搬到 TsFile，便于后续用 [TimechoDB](https://www.timecho.com/) / Apache IoTDB / TsFile SDK 继续存储与分析。

## 下载

发行包在 [Releases](https://github.com/TimechoLab/influxdb2tsfile/releases) 页面：

| 文件 | 说明 |
| --- | --- |
| `influxdb2tsfile-<version>-bin.tar.gz` | 解压即用：`bin/influxdb2tsfile` 启动脚本 + `lib/` 下的 fat-jar + 中英文 README + LICENSE |
| `influxdb2tsfile-<version>.jar` | 单独的 fat-jar（含全部依赖） |

```bash
tar xzf influxdb2tsfile-1.0.0-bin.tar.gz && cd influxdb2tsfile-1.0.0
export JAVA_HOME=/path/to/jdk-17      # 需要 JDK 17 及以上
bin/influxdb2tsfile --help
```

## 特性

- 支持 **InfluxDB 1.x**（InfluxQL + 用户名/密码）与 **InfluxDB 2.x**（InfluxQL 兼容接口 + Token），两者命令与参数一致
- 支持 **离线 line protocol 文件**（`lp2tsfile` 命令）：实例已下线时，可用 `influxd export` / `influx export` 导出的 line protocol 文件完成迁移
- 自动发现 schema 并映射：measurement → 表，tag → TAG 列，field → FIELD 列，字段类型自动映射（float→DOUBLE、integer/unsigned→INT64、boolean→BOOLEAN、string→STRING）
- 流式读取 + 分片写入：时间切片（`--time-slice`）、按大小/行数滚动（`--max-file-size` / `--max-rows-per-file`），内存占用可预估
- 并行迁移（`--parallel`）：每个「工作单元」独立输出文件，互不干扰
- 断点续传（`--resume`）：失败的单元重跑，已完成单元跳过，不产生重复数据
- 内置校验：`--verify` 与 `inspect --manifest` 会回读 TsFile 行数并与清单比对
- 生成 `manifest.json` 迁移清单：逐单元记录文件、行数、时间范围、列映射，便于审计与二次开发
- 单个 fat-jar，除 JRE 外无外部依赖

## 环境要求

| 项目 | 要求 |
| --- | --- |
| JDK | **17 及以上**（构建与运行）；TsFile 2.4.0 的字节码是 Java 17，低于 17 会报 `UnsupportedClassVersionError` / `class file version 61.0` |
| Maven | 3.6 及以上（仅构建需要） |
| TsFile | 2.4.0（已作为依赖内置） |

启动脚本 `bin/influxdb2tsfile` 会先检查 JDK 版本并给出明确提示，避免难懂的 `UnsupportedClassVersionError`。

## 构建

```bash
mvn -DskipTests package      # 生成 target/influxdb2tsfile-1.0.0.jar（含全部依赖）
mvn test                     # 单元测试 + 集成测试（内置 mock InfluxDB，无需真实实例）
```

## 快速开始

### 1. 查看 InfluxDB 中的 schema 与映射关系

```bash
java -jar target/influxdb2tsfile-1.0.0.jar discover \
  --url http://127.0.0.1:8086 --database telegraf --counts
```

### 2. 迁移数据

```bash
java -jar target/influxdb2tsfile-1.0.0.jar migrate \
  --url http://127.0.0.1:8086 --database telegraf \
  --start -90d --end now \
  --time-slice 1d --parallel 4 \
  -o /data/tsfile-out --verify
```

InfluxDB 2.x 只需换成 Token 认证：

```bash
java -jar target/influxdb2tsfile-1.0.0.jar migrate \
  --url http://127.0.0.1:8086 --database telegraf --token <api-token> \
  --time-slice 1d --parallel 4 -o /data/tsfile-out --verify
```

### 3. 从 line protocol 文件迁移（离线场景）

```bash
java -jar target/influxdb2tsfile-1.0.0.jar lp2tsfile /backup/export.lp -o /data/tsfile-out
cat /backup/export.lp | java -jar target/influxdb2tsfile-1.0.0.jar lp2tsfile - -o /data/tsfile-out
```

### 4. 查看与校验生成的 TsFile

```bash
java -jar target/influxdb2tsfile-1.0.0.jar inspect /data/tsfile-out --rows 3
java -jar target/influxdb2tsfile-1.0.0.jar inspect /data/tsfile-out --manifest /data/tsfile-out/manifest.json
```

完整说明见 **[用户手册](docs/用户手册.md)**，设计说明见 **[设计文档](docs/DESIGN.md)**。

## 命令一览

| 命令 | 作用 | 常用参数 |
| --- | --- | --- |
| `migrate` | 从 InfluxDB 读取并写出 TsFile | `--measurements`、`--measurement-regex`、`--start/--end`、`--time-slice`、`--parallel`、`--resume`、`--verify`、`--dry-run` |
| `discover` | 打印 measurement/tag/field 与 TsFile 映射 | `--counts`、`--json`、`--report <file>` |
| `lp2tsfile` | line protocol 文件或 stdin 转 TsFile | `--precision`、`--default-time`、`--skip-invalid` |
| `inspect` | 查看表、列、行数、设备数、时间范围、样本行 | `--rows N`、`--json`、`--manifest <file>` |

任意命令加 `--help` 查看全部参数。通用写入参数：`--max-file-size`、`--batch-size`、`--compression`
（UNCOMPRESSED/SNAPPY/GZIP/LZ4/ZSTD/LZMA2）、`--no-sanitize`、`--exclude-tags`、`--exclude-fields`、
`--field-type`、`--type-conflict`。

## 数据映射规则

| InfluxDB | TsFile（表模型 table model） |
| --- | --- |
| database / bucket | 输出目录（可用 `--table-prefix` 把库名并入表名前缀） |
| measurement | 表名 table |
| tag key / value | TAG 列（STRING），一行数据的 tag 组合构成设备 ID（series） |
| field key | FIELD 列，类型：float→DOUBLE、integer/unsigned→INT64、boolean→BOOLEAN、string→STRING |
| timestamp | 行时间戳（纳秒，TsFile 原生时间列） |
| 该行缺失的 field | 该列为 null（bitmap 标记，不占数据空间） |

## 可靠性

- 一次迁移被切成若干**工作单元**（measurement × 时间切片），一个单元只写自己的文件：可以并行、可单独重跑、不混数据；
- 相同参数重复执行是**幂等**的：单元写入前会先清掉自己名下的分片文件；
- `--resume` 会读取上次的 manifest，并**重新校验记录中的产物文件**（是否存在、大小是否一致），只补做缺失/失败/失效的单元；
- `--verify`（或事后 `inspect --manifest`）回读所有产物，按表比对行数，不一致退出码 1。

## 输出文件

```
/data/tsfile-out/
├── cpu_00000-p0000.tsfile      # measurement cpu，第 0 个时间切片，第 0 个分片
├── cpu_00000-p0001.tsfile      #   该切片超过 --max-file-size 后滚动出的第 2 个文件
├── cpu_00001-p0000.tsfile      # 第 1 个时间切片
├── mem_00000-p0000.tsfile
└── manifest.json               # 逐单元的文件、行数、设备数、时间范围、列映射
```

## 目录结构

```
src/main/java/com/timecho/influxdb2tsfile/
├── cli/       命令行入口（migrate、discover、lp2tsfile、inspect）
├── core/      名称规范化、时间/大小/时长解析、schema 规划、写入选项
├── source/    数据源：InfluxDB(HTTP + InfluxQL CSV)、line protocol
├── sink/      TsFile 写入器与迁移清单（manifest）
├── inspect/   TsFile 读取与统计
└── pipeline/  工作单元、清单组装、校验
```

## 已知限制

- 只迁移时序数据；InfluxDB 的元数据（用户、看板、连续查询、订阅、任务）不在范围内；
- 一个 measurement 对应一张表；多个 database/bucket 迁到同一目录需用 `--table-prefix` 区分；
- 默认只读默认保留策略，其它 RP 需显式用 `--rp` 单独迁移；
- 同一字段在 InfluxDB 中类型不一致时，需用 `--type-conflict text` 按字符串迁移，或在源端修复；
- TsFile 表模型列名大小写不敏感（统一小写，原名记录在 manifest 中）；
- 只提供命令行，不提供 Web 界面（有意为之）。

## 文档

- [用户手册](docs/用户手册.md)：阿里云退市迁移流程、全量参数说明、校验方法、FAQ、已知限制
- [设计文档](docs/DESIGN.md)：架构、映射规格、工作单元与清单模型、错误处理

## License

Apache License 2.0