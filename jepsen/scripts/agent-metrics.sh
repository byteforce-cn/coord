#!/bin/bash
# agent-metrics.sh —— 抓 coord-agent 的 /metrics。
#
# M5a 的**路由证明**（AG-01）与 agent 就绪探测都用它，两个场景共用同一份实现：
#   * 在 **agent 节点**上：`agent-metrics.sh 19528`（探本机 loopback）
#   * 在 **控制机**上：`agent-metrics.sh 19601`（探隧道端口 = 端到端证明）
#
# 为什么用脚本文件而不是 `c/exec` 的内联一行：
#   内联要经过 jepsen.control 的转义 + sudo + 外层 bash -c 三层引号，实测
#   `/dev/tcp` 的重定向在这种嵌套下会失败（`bash: line 1: /dev/tcp/...:
#   No such file or directory`，而同样的一行在节点上交互执行是完全正常的）。
#   上传一个文件即彻底消掉这一类转义脆弱性。
#
# 为什么用 bash 内建 /dev/tcp 而不是 curl/wget/nc：
#   lab 镜像里这三者都不保证存在；bash 一定在（/dev/tcp 是 bash 的内建重定向）。
#
# 退出码：0 = 拿到响应（内容是否含指标由调用方判）；1 = 连接失败。
set -u

PORT="${1:-19528}"

exec 3<>"/dev/tcp/127.0.0.1/${PORT}" || exit 1
printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3
timeout 5 cat <&3 || true
