# influxdb2tsfile 设计文档

版本 1.0.0 / Apache TsFile 2.4.0

## 1. 目标与范围

**目标**：提供一个零外部依赖（除 JRE 外）的命令行工具，把 InfluxDB 1.x/2.x 以及 line protocol 文件中的时序数据，完整、可校验地转换成 Apache TsFile 2.4.0 的表模型文件。

**范围内的设计取舍**：

| 决策 | 原因 |
| --- | --- |
| 命令行工具，不做 Web 界面 | 迁移是一次性/批处理任务，脚本化与可重复执行比可视化更重要 |
| 只用 JDK 内置的 `java.net.http.HttpClient`（Java 11 引入）访问 InfluxDB，不引入官方 SDK | 官方 1.x/2.x SDK 依赖重（okhttp/reactor），且需要同时支持两种版本；本工具只需 HTTP + CSV 解析 |
| 只使用 Python 无关的 Java 生态：picocli（CLI）、Jackson（manifest JSON）、TsFile SDK | 依赖少、fat-jar 小（约 15MB）、构建快 |
| 不引入 Spring Boot | 单进程批处理工具，Spring 只会增加启动时间与体积（YAGNI） |
| 数据读取走 **InfluxQL + CSV**（1.x 与 2.x 的 `/query` 兼容接口） | 两种版本统一一条读取路径；CSV 有类型后缀（`1i`/`1u`/引号/布尔），比 JSON 更能无损还原类型；流式返回、内存可控 |
| 输出 **表模型**（measurement→表）而不是树模型 | 与 InfluxDB 的 measurement/tag/field 语义最贴近；TsFile 表模型列式对齐、按设备组织，便于后续 SQL/加载 |

## 2. 总体架构

```
                          ┌────────────────────────────┐
   InfluxDB 1.x/2.x ─────▶│ source/influx              │
   (HTTP + InfluxQL CSV)  │  InfluxClient  (HTTP/认证)  │
                          │  InfluxCsvParser (流式 CSV) │
                          │  InfluxSource  (探查/查询)  │──┐
                          └────────────────────────────┘  │
   line protocol 文件 ───▶┌────────────────────────────┐  │   RowSink
   / stdin                │ source/lp                  │  │  (measurement, tags,
                          │  LineProtocolParser        │──┤   timeNanos, fields)
                          │  LineProtocolSource(两遍扫描)│  │
                          └────────────────────────────┘  │
                                                          ▼
   core:   SchemaPlanner(探查结果 → TableSpec)      pipeline: MigrationUnit / ManifestSupport / Verifier
   sink:   UnitWriter(设备缓冲 + Tablet 批量写 + 文件滚动) ──▶ *.tsfile + manifest.json
   inspect:TsFileInspector(回读表结构/行数/时间范围/样本)  ──▶ CLI 文本或 JSON
```

关键接口：

- `RowSink.row(measurement, tags, timeNanos, fields)`：数据源统一出口，一行就是一个 InfluxDB 点；
- `TableSpec`：一张表的完整定义（表名、TAG 列、FIELD 列、类型、列名映射），负责构造 TsFile 的 `TableSchema` 与 `Tablet`；
- `UnitWriter`：一个工作单元（measurement [+ 时间切片]）的写入器，内部按设备缓冲、批量写 Tablet、按大小/行数滚动文件。

## 3. 数据映射规格

### 3.1 从 InfluxDB 到 TsFile

| InfluxDB | TsFile 表模型 | 实现位置 |
| --- | --- | --- |
| measurement | table name（小写规范化） | `SchemaPlanner.plan` |
| tag key/value | TAG 列（STRING） | 同上；tag 顺序 = 排序后的 tag key 集合 |
| field key | FIELD 列 | 同上 |
| field type | DOUBLE/INT64/BOOLEAN/STRING | `InfluxType` |
| timestamp | 行时间戳（int64 纳秒） | `RowSink` → `Tablet.addTimestamp` |
| series（tag 组合） | device id（tableName + tag 值数组） | `IDeviceID.Factory.create(String[])` |
| 缺失 field | 列值为 null（Tablet bitmap 标记） | `Tablet.addValue` 只对非空列调用 |
| 缺失 tag（同一 measurement 内 tag 稀疏） | 空字符串 `""` | 保证 device id 稳定、可重复 |

### 3.2 写入路径

```
InfluxDB CSV 行 ─▶ RowSink ─▶ UnitWriter.addRow()
                                 │  按 (table, tag 值) 找到/新建 DeviceBuffer
                                 │  值按列类型 coerce（TableSpec.coerce）
                                 ▼
                        DeviceBuffer（按时间追加，必要时排序）
                                 │  行数达到 --batch-size
                                 ▼
                        Tablet（列式数组 + 时间列）
                                 │  TsFileWriter.writeTable(tablet, devicePair)
                                 ▼
                        TsFile 分片文件（超过 --max-file-size/--max-rows-per-file 滚动）
```

要点：

1. **设备内时间有序**：InfluxDB 返回的同一个 series 必然按时间递增；LineProtocolSource 允许文件内局部乱序，`DeviceBuffer.drainSorted` 在刷盘时对同一设备的行排序（就近有序时几乎零成本）；
2. **一个 Tablet 一个设备**：调用 `writeTable(tablet, pairs)` 直接给出设备与结束行号，避免框架逐行推导；
3. **tag 缓存**：CSV 是按 series 连续输出的，`UnitWriter` 缓存「上一个 series 的 tag 值数组」，避免每行重复构造；
4. **空值不写数据**：`Tablet` 第一次写值时会初始化 bitmap 并全部标记为 null，之后只有非空列被 unset，因此稀疏数据不会浪费空间。

### 3.3 名称规范化

`core/Naming`：小写化 → 非 [a-z0-9_汉] 字符替换为 `_` → 合并连续 `_` → 去首尾 `_` → 空名 `col` → 保留字 `time` 改 `time_col` → 冲突加序号。原名与实际列名的映射写入 manifest 的 `tables[].columns[].source`。

## 4. 工作单元与并发模型

**工作单元（unit）= 一个 measurement + 一个时间切片**（未指定 `--time-slice` 时就是整个 measurement）。

- 单元名（同时是文件名的一部分）：`<sanitized measurement>[_<5 位切片序号>]`，例如 `cpu_00003`；
- 一个单元只写自己名下的 `<prefix>-<unit>-pNNNN.tsfile`，单元之间文件互不重叠 → **并发安全、可隔离重跑**；
- 切片边界根据**数据的实际时间范围**对齐（先各查一次 `ORDER BY time ASC/DESC LIMIT 1`），避免从 1970 年开始切出成千上万个空切片；
- `--parallel N` 用固定线程池跑单元，每个线程一个 `UnitWriter`，无共享可变状态（仅 progress 计数与 manifest 写入加锁）；
- 单元失败 → 记录 `status=failed` + `error`，可选择 `--fail-fast` 停止；重试前会删除该单元残留文件，**不会产生重复行**（同参数重跑幂等）。

## 5. 断点续传（manifest 即检查点）

- 每完成一个单元就地更新 `manifest.json`（原子写：临时文件 + ATOMIC_MOVE；写入节流为「距上次保存 > 2s 或状态非 done」）；
- `--resume` 读取已有清单，跳过 `status=done/empty` 的单元，只跑未完成/失败的单元；
- 强杀进程最多丢一个正在写的单元，重跑时会先清理该单元的分片文件。

## 6. 校验

| 层次 | 手段 | 说明 |
| --- | --- | --- |
| 写入后 | `migrate --verify` | 回读全部产物，按表汇总行数，与 manifest 对比，任何不一致 → 退出码 1 |
| 独立 | `inspect <dir> --manifest manifest.json` | 可随时重复执行，检查文件是否齐全、行数是否一致 |
| 明细 | `inspect --rows N --json` | 打印每张表的列、设备数、时间范围、样本行，便于抽样比对 |

## 7. 失败与错误处理

| 场景 | 行为 |
| --- | --- |
| 连接失败/地址错误 | 立即失败，提示检查 URL/网络 |
| 认证失败（401/403） | 立即失败，提示检查 token 或用户名密码 |
| 2.x 缺 DBRP 映射（database not found） | 失败并提示用 `influx v1 dbrp create` 建映射 |
| 查询超时/网络抖动 | 单元级重试（`--retries`），仍失败则记录到 manifest |
| 字段类型冲突 | 默认跳过该 measurement 并报错；`--type-conflict text` 转字符串 |
| unsigned 超出 INT64 | 该单元失败，提示改用 `--field-type <field>=STRING` |
| line protocol 非法行 | 默认报错并给出行号；`--skip-invalid` 跳过并统计 |
| 切片过多（> 20 万） | 直接报错，提示缩小 `--start/--end` |

## 8. 兼容性与版本

- TsFile 2.4.0（`org.apache.tsfile:tsfile:2.4.0`）：表模型 API（`TableSchema`/`Tablet`/`TsFileWriter.writeTable`、`TsFileReaderBuilder`）；默认压缩 LZ4；
- JDK 17 起（`maven.compiler.release=17`，与 TsFile 2.4.0 的字节码版本一致）：使用 `java.net.http.HttpClient` 等 JDK 内置 API，不引入 Spring 等重框架；
- 构建：Maven 3.6+，依赖 picocli 4.7.7、Jackson 2.21.5、slf4j 2.0.17，测试用 JUnit 5.9.3；
- 测试：单元测试（解析/规划/命名）+ 集成测试（内置 mock InfluxDB + 真实 TsFile 读写回环，覆盖迁移、校验、失败续传）。

## 9. 后续可扩展点

1. Flux 查询模式（2.x 原生 `/api/v2/query` 注解 CSV 已实现解析器，可用于按字段分组读取）；
2. 直接读取 InfluxDB 备份目录（`influxd backup -portable` 的 TSDB 文件）以获得更高吞吐；
3. 导出更多清单/指标（Prometheus 文本、CSV 报告）；
4. 写入端支持 IoTDB/TimechoDB 直连（当前只输出文件，交由 `load tsfile` 导入）。