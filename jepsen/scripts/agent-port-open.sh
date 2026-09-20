#!/bin/bash
# agent-port-open.sh —— 探「某个 loopback 端口是否在监听」。
#
# 用法（在 **agent 节点**上）：`agent-port-open.sh 19527`
# 退出码：0 = TCP 连上了；1 = 连不上。
#
# 为什么需要它（M5a 第二轮矩阵的实测教训）：
#   coord-agent 的启动顺序是「先起 HTTP（/metrics、/health），再连 server 集群，
#   **连上之后**才起 gRPC listener」—— 实测 HTTP 在启动后 ~0.3s 就在监听，而
#   gRPC 端口要等 ~9s（`coord-agent connected to server cluster (attempt 1)`）。
#   而隧道与客户端要的都是 **gRPC 端口**。只等 HTTP 就会得到一个「看起来就绪、
#   实际连不上」的窗口 —— 表现是
#     `tunnel a2 to n5 did not come up ... Connection refused`
#   （一轮 9 个 cell 全部在这个窗口里挂掉，而且看起来像 agent 的缺陷）。
#
# 为什么用 bash 内建 /dev/tcp 而不是 curl/nc/ss：
#   lab 镜像里这三者都不保证存在（jepsen-control 里连 ss 都没有），bash 一定有。
#   与 agent-metrics.sh 同一取向：上传一个文件，消掉嵌套引号/转义那一整类脆弱性。
set -u
exec 3<>"/dev/tcp/127.0.0.1/${1:-19527}"
