#!/usr/bin/env bash
# 打包「下载即可运行」的发行包：dist/influxdb2tsfile-<version>-bin.tar.gz
# 用法: bash bin/make-dist.sh [version]    （默认取 pom.xml 里的项目版本）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

VERSION="${1:-$(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' pom.xml | head -1)}"
JAR="target/influxdb2tsfile-${VERSION}.jar"

if [ ! -f "$JAR" ]; then
  echo "未找到 ${JAR}，开始构建 ..."
  mvn -B -ntp -DskipTests package
fi

STAGING="dist/influxdb2tsfile-${VERSION}"
rm -rf "$STAGING"
mkdir -p "$STAGING/bin" "$STAGING/lib"
cp bin/influxdb2tsfile "$STAGING/bin/"
cp "$JAR" "$STAGING/lib/"
cp README.md README.zh.md LICENSE "$STAGING/"

TARBALL="dist/influxdb2tsfile-${VERSION}-bin.tar.gz"
rm -f "$TARBALL"
tar -C dist -czf "$TARBALL" "influxdb2tsfile-${VERSION}"

echo "发行包: ${TARBALL}"
if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$TARBALL" | sed 's:dist/::'
elif command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$TARBALL" | sed 's:dist/::'
fi
echo "内容:"
tar -tzf "$TARBALL" | sed 's/^/  /'
