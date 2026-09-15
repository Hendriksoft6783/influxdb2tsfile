#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""向 InfluxDB 1.x 写入演示数据（幂等：相同时间戳重复写入即覆盖）。

用法:
  python3 /Volumes/data/sources/influxdb2tsfile/examples/load-demo-data.py
环境变量: INFLUX_URL / INFLUX_DB / INFLUX_USER / INFLUX_PASSWORD
"""
import base64
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("INFLUX_URL", "http://127.0.0.1:18086").rstrip("/")
DB = os.environ.get("INFLUX_DB", "demo")
USER = os.environ.get("INFLUX_USER", "admin")
PASSWORD = os.environ.get("INFLUX_PASSWORD", "")
TOKEN = os.environ.get("INFLUX_TOKEN", "")

AUTH = {}
if TOKEN:
    # InfluxDB 2.x: Token 认证 + v1 兼容接口写数据（db 参数即 bucket）
    AUTH["Authorization"] = "Token " + TOKEN
elif USER:
    token = base64.b64encode((USER + ":" + PASSWORD).encode()).decode()
    AUTH["Authorization"] = "Basic " + token


def query(statement):
    params = urllib.parse.urlencode({"q": statement, "db": DB})
    request = urllib.request.Request(BASE + "/query?" + params)
    for key, value in AUTH.items():
        request.add_header(key, value)
    request.add_header("Accept", "application/csv")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def api_v2(path, payload=None, method="GET"):
    """InfluxDB 2.x 原生 API（用于清空 bucket）。"""
    request = urllib.request.Request(
        BASE + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method)
    request.add_header("Authorization", "Token " + TOKEN)
    if payload is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def clear_bucket():
    """删除 bucket 中的历史数据，保证演示数字可复现。"""
    org = os.environ.get("INFLUX_ORG", "myorg")
    status, body = api_v2("/api/v2/buckets?org=%s&name=%s" % (org, DB))
    buckets = json.loads(body).get("buckets", []) if status == 200 else []
    if not buckets:
        raise SystemExit("bucket 不存在: %s（请先创建或用 INFLUX_DB 指定）" % DB)
    stop = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 86400))
    status, body = api_v2(
        "/api/v2/delete?org=%s&bucket=%s" % (org, DB),
        {"start": "1970-01-01T00:00:00Z", "stop": stop},
        "POST")
    print("清空 bucket %s 历史数据 -> HTTP %s" % (DB, status))


def write(lines):
    body = ("\n".join(lines) + "\n").encode()
    params = urllib.parse.urlencode({"db": DB, "precision": "ns"})
    request = urllib.request.Request(BASE + "/write?" + params, data=body, method="POST")
    for key, value in AUTH.items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request) as response:
            return response.status
    except urllib.error.HTTPError as error:
        detail = error.read().decode()
        print("写入失败 HTTP %s: %s" % (error.code, detail))
        for line in lines[:3]:
            print("  示例行: %s" % line)
        raise


# 每次演示都从干净的库开始，保证数字可复现
if TOKEN:
    print("InfluxDB 2.x（Token 模式）: 使用已有 bucket %s" % DB)
    clear_bucket()
    time.sleep(2)
else:
    print("drop database %s -> %s" % (DB, query('DROP DATABASE "%s"' % DB)[0]))
    print("create database %s -> %s" % (DB, query('CREATE DATABASE "%s"' % DB)[0]))

# 数据锚定在"最近 24 小时"，这样 InfluxDB UI / Chronograf 默认的 "Past 1h" 时间窗就能看到曲线
import time as _time

STEP = 60
POINTS = 1440                                                  # 每分钟一个点，共 24 小时
# 再往前留 2 分钟，避免"最近一个点"落在查询结束时间（now）之后
START = int(_time.time()) // STEP * STEP - (POINTS - 1) * STEP - 2 * STEP
HOSTS = ["host-a", "host-b", "host-c", "host-d"]
REGIONS = {"host-a": "cn", "host-b": "cn", "host-c": "us", "host-d": "us"}
PATHS = ["/data", "/tmp"]

random.seed(20240101)
lines = []
for i in range(POINTS):
    ts = (START + i * STEP) * 10 ** 9
    for host in HOSTS:
        region = REGIONS[host]
        idle = 90 + random.random() * 8
        user = 3 + random.random() * 6
        cores = 8 if host in ("host-a", "host-b") else 16
        online = "true" if random.random() > 0.02 else "false"
        lines.append(
            "cpu,host=%s,region=%s usage_idle=%.3f,usage_user=%.3f,online=%s,cores=%di %d"
            % (host, region, idle, user, online, cores, ts)
        )
        # host-b 有一条只有 zone 标签、只带两个字段的 series（演示稀疏 tag/field）
        if host == "host-b":
            lines.append(
                "cpu,host=%s,zone=z1 usage_idle=%.3f,usage_user=%.3f %d" % (host, idle, user, ts)
            )
        lines.append(
            "mem,host=%s used_percent=%.2f,cached=%di,swap_used=%di %d"
            % (host, 40 + random.random() * 30, 10 ** 7 + i, 1024 + i, ts)
        )
        for path in PATHS:
            if path == "/tmp" and i % 3 != 0:
                continue
            lines.append(
                "disk,host=%s,path=%s free=%di,used=%di,inodes_free=%di %d"
                % (host, path, 10 ** 10 + i, 10 ** 9 + i, 10 ** 6 - i, ts)
            )
    # 传感器：字符串/布尔字段 + 30 秒采样
    for slot in range(2):
        device = "sensor-%02d" % (slot + 1)
        sub_ts = ts + slot * 30 * 10 ** 9
        status_text = "ok" if random.random() > 0.05 else "warning: high"
        lines.append(
            'sensor,device_id=%s,location=room%d temperature=%.2f,humidity=%.2f,battery=%di,status="%s",alert=%s %d'
            % (device, slot + 1, 18 + random.random() * 10, 30 + random.random() * 40,
               90 + slot, status_text, "false" if random.random() > 0.05 else "true", sub_ts)
        )

batch = 2000
written = 0
for index in range(0, len(lines), batch):
    chunk = lines[index:index + batch]
    if write(chunk) != 204:
        raise SystemExit("写入失败")
    written += len(chunk)

print("写入点数: %d" % written)
for measurement in ("cpu", "mem", "disk", "sensor"):
    status, body = query('SELECT COUNT(*) FROM "%s" GROUP BY *' % measurement)
    print("---- %s ----" % measurement)
    print(body.strip())