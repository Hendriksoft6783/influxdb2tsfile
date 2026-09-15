#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从演示产物生成 SUMMARY.md（供汇报/推文使用）。"""
import json
import os
import sys

FENCE = chr(96) * 3


def main():
    out = sys.argv[1]
    manifest = json.load(open(os.path.join(out, "10-manifest.json"), encoding="utf-8"))
    totals = manifest["totals"]
    rows_by_table = {}
    for unit in manifest["units"]:
        for table, stat in unit.get("tables", {}).items():
            rows_by_table[table] = rows_by_table.get(table, 0) + stat["rows"]

    lines = []
    lines.append("# InfluxDB -> Apache TsFile 迁移演示结果")
    lines.append("")
    lines.append("| 项目 | 结果 |")
    lines.append("| --- | --- |")
    lines.append("| 源 | InfluxDB %s, database=%s |" % (manifest.get("sourceType"), manifest.get("database")))
    lines.append("| 目标 | Apache TsFile %s（表模型 + LZ4 压缩） |" % manifest.get("tsfileVersion"))
    lines.append("| 时间切片 / 并发 | %s / %s |" % (manifest["options"].get("timeSlice"), manifest["options"].get("parallel")))
    lines.append("| 工作单元 | %d 个（已完成 %d，失败 %d） |" % (totals["units"] + totals["failedUnits"], totals["units"], totals["failedUnits"]))
    lines.append("| 总行数（= 数据点数） | %d |" % totals["rows"])
    lines.append("| tsfile 分片 | %d 个，共 %.1f KB |" % (totals["files"], totals["bytes"] / 1024.0))
    lines.append("")
    lines.append("## 表结构映射")
    lines.append("")
    lines.append("| 表（measurement） | 行数 | 列 |")
    lines.append("| --- | ---: | --- |")
    for table in manifest["tables"]:
        columns = ", ".join("%s(%s)" % (c["column"], c["category"]) for c in table["columns"])
        lines.append("| %s | %d | %s |" % (table["table"], rows_by_table.get(table["table"], 0), columns))
    lines.append("")
    lines.append("## 演示命令（绝对路径）")
    lines.append("")
    lines.append(FENCE + "bash")
    lines.append("# 1. 构建（生成 target/influxdb2tsfile-0.1.0.jar）")
    lines.append("mvn -DskipTests package -f /Volumes/data/sources/influxdb2tsfile/pom.xml")
    lines.append("")
    lines.append("# 2. 探查 InfluxDB schema 与 TsFile 列映射")
    lines.append("java -jar /Volumes/data/sources/influxdb2tsfile/target/influxdb2tsfile-0.1.0.jar discover \\")
    lines.append("  --url http://127.0.0.1:18086 --database demo --username admin --password admin123 --counts")
    lines.append("")
    lines.append("# 3. 迁移：6 小时切片、4 并发、断点续传、写入后自动校验")
    lines.append("java -jar /Volumes/data/sources/influxdb2tsfile/target/influxdb2tsfile-0.1.0.jar migrate \\")
    lines.append("  --url http://127.0.0.1:18086 --database demo --username admin --password admin123 \\")
    lines.append("  --time-slice 6h --parallel 4 --resume --verify \\")
    lines.append("  -o /Volumes/data/sources/influxdb2tsfile/demo-output/influx-demo")
    lines.append("")
    lines.append("# 4. 回读产物：表结构、行数、设备数、时间范围、样本行")
    lines.append("java -jar /Volumes/data/sources/influxdb2tsfile/target/influxdb2tsfile-0.1.0.jar inspect \\")
    lines.append("  /Volumes/data/sources/influxdb2tsfile/demo-output/influx-demo --rows 3")
    lines.append("")
    lines.append("# 5. 校验：产物行数与 manifest 清单比对（不一致退出码 1）")
    lines.append("java -jar /Volumes/data/sources/influxdb2tsfile/target/influxdb2tsfile-0.1.0.jar inspect \\")
    lines.append("  /Volumes/data/sources/influxdb2tsfile/demo-output/influx-demo \\")
    lines.append("  --manifest /Volumes/data/sources/influxdb2tsfile/demo-output/influx-demo/manifest.json")
    lines.append("")
    lines.append("# 6. 离线兜底：line protocol 文件直接转 TsFile")
    lines.append("java -jar /Volumes/data/sources/influxdb2tsfile/target/influxdb2tsfile-0.1.0.jar lp2tsfile \\")
    lines.append("  /Volumes/data/sources/influxdb2tsfile/examples/sample.lp \\")
    lines.append("  -o /Volumes/data/sources/influxdb2tsfile/demo-output/lp-demo")
    lines.append(FENCE)
    lines.append("")
    lines.append("## 产物文件")
    lines.append("")
    for name in sorted(os.listdir(os.path.join(out, "influx-demo"))):
        lines.append("- influx-demo/%s" % name)
    lines.append("")
    text = "\n".join(lines)
    open(os.path.join(out, "SUMMARY.md"), "w", encoding="utf-8").write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
