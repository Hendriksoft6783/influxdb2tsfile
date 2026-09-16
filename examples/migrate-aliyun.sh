#!/usr/bin/env bash
# 示例：把阿里云 InfluxDB（1.x 兼容接口）迁移为 TsFile
#   1. discover  查看 measurement 与映射关系
#   2. dry-run   只打印迁移计划（工作单元、切片数），不读数据
#   3. migrate   正式迁移，可反复执行（--resume 断点续传）
#   4. verify    回读 TsFile 统计行数并与清单比对
set -euo pipefail

JAR="$PWD/target/influxdb2tsfile-1.0.0.jar"
URL="http://my-influxdb-host:8086"
DB="telegraf"
USER="admin"
PASS="admin123"
OUT="/data/tsfile/$DB"

java -jar "$JAR" discover --url "$URL" --database "$DB" --username "$USER" --password "$PASS"

java -jar "$JAR" migrate --url "$URL" --database "$DB" --username "$USER" --password "$PASS" \
  -o "$OUT" --time-slice 1d --parallel 4 --dry-run

java -jar "$JAR" migrate --url "$URL" --database "$DB" --username "$USER" --password "$PASS" \
  -o "$OUT" --time-slice 1d --parallel 4 --resume --verify

java -jar "$JAR" inspect "$OUT" --manifest "$OUT/manifest.json"
