# 集群部署（三节点 Raft）

生产拓扑参考，基于 Docker Compose，三节点 Raft 多数写入，容忍 1 节点故障。

---

## 一、启动集群

```bash
docker compose -f docker/docker-compose.cluster.yml up -d

# 指定版本
COORD_VERSION=0.1.10 docker compose -f docker/docker-compose.cluster.yml up -d
```

### 节点端口映射

| 节点 | gRPC（宿主机） | HTTP（宿主机） |
|------|---------------|---------------|
| coord-1 | `9090` | `9091` |
| coord-2 | `19090` | `19091` |
| coord-3 | `29090` | `29091` |

---

## 二、集群自举流程

1. `coord-1` 以 `COORD_BOOTSTRAP=true` 启动，成为初始 Raft leader（单节点模式）。
2. `coord-2` / `coord-3` 等待 `coord-1` 健康后启动。
3. `coord-1` 探测 `coord-2:9090` / `coord-3:9090`，通过 auto-join 将它们加入集群。
4. 三节点 quorum 建立，集群进入正常服务状态。

### 验证集群就绪

```bash
curl http://localhost:9091/healthz   # coord-1
curl http://localhost:19091/healthz  # coord-2
curl http://localhost:29091/healthz  # coord-3

# 查看 Raft 状态
coord ctl cluster status
```

---

## 三、安全域初始化（集群场景）

安全域初始化只需对集群 **leader** 执行一次（通常是 coord-1）：

```bash
coord ctl --endpoint http://127.0.0.1:9090 \
  operator init --shares-total 5 --threshold 3
```

保存 5 个 shares 和 root_token，然后提交 3 个 share 解封：

```bash
coord ctl --endpoint http://127.0.0.1:9090 operator unseal shamir:AAAA...
coord ctl --endpoint http://127.0.0.1:9090 operator unseal shamir:BBBB...
coord ctl --endpoint http://127.0.0.1:9090 operator unseal shamir:CCCC...
# → sealed: false
```

> **集群重启后**，每个节点独立处于 sealed 状态，需对每个节点各提交 threshold 份额：
>
> ```bash
> for PORT in 9090 19090 29090; do
>   coord ctl --endpoint http://127.0.0.1:$PORT operator unseal shamir:AAAA...
>   coord ctl --endpoint http://127.0.0.1:$PORT operator unseal shamir:BBBB...
>   coord ctl --endpoint http://127.0.0.1:$PORT operator unseal shamir:CCCC...
> done
> ```

---

## 四、手动管理成员

```bash
# 查看当前成员
coord ctl cluster status

# 新增节点
coord ctl member add coord-4 coord-4:9090

# 移除节点（优雅下线）
coord ctl member remove coord-4

# 移除不可达节点（强制）
coord ctl member remove coord-4 --force-unreachable
```

---

## 五、自定义 Compose 配置

```yaml
services:
  coord-1:
    image: nexus.byteforce.cn/image-private/coord:0.1.11
    command: ["server"]
    environment:
      COORD_NODE_ID: "node-1"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_CLUSTER_PEERS: "coord-2:9090,coord-3:9090"
      COORD_BOOTSTRAP: "true"
      COORD_DATA_DIR: "/data"
      # TLS（可选）
      COORD_TLS_CERT: "/certs/server.crt"
      COORD_TLS_KEY: "/certs/server.key"
      COORD_TLS_CLIENT_CA: "/certs/ca.crt"
    volumes:
      - coord-data-1:/data
      - ./certs:/certs:ro
```

---

## 六、备份与恢复

```bash
# 创建备份（快照写入本地文件）
coord ctl backup create --file coord-backup.json

# 恢复（危险！会覆盖当前状态，操作前务必停服或确认数据）
coord ctl backup restore coord-backup.json
```

---

## 七、停止并清理

```bash
# 保留数据卷
docker compose -f docker/docker-compose.cluster.yml down

# 彻底清空（删除数据）
docker compose -f docker/docker-compose.cluster.yml down -v
```

---

## 八、生产注意事项

- **Raft 容忍性**：3 节点可容忍 1 节点故障；5 节点可容忍 2 节点故障。
- **网络延迟**：节点间 RTT 建议 ≤ 10 ms，Raft tick 周期 100 ms。
- **数据目录**：使用块存储（SSD）挂载 `/data`，不要使用 NFS。
- **时钟同步**：所有节点 NTP 偏差应 ≤ 500 ms，否则影响锁 TTL 精度。
- **端口防火墙**：节点间 gRPC 端口（默认 9090）须互通；HTTP 控制面（9091）对外暴露时建议加 Nginx/mTLS 限制。
