# 集群部署（三节点 Raft）

生产拓扑参考，基于 Docker Compose，三节点 Raft 多数写入，容忍 1 节点故障。本文直接给出可复制执行的 `docker-compose.yml`。

---

## 一、创建 `docker-compose.yml` 并启动

```bash
cat > docker-compose.yml <<'YAML'
name: coord-cluster

services:
  coord-1:
    image: nexus.byteforce.cn/image-private/coord:${COORD_VERSION:-0.1.14}
    container_name: coord-1
    hostname: coord-1
    command: ["server"]
    restart: unless-stopped
    networks:
      - coord-net
    ports:
      - "9090:9090"
      - "9091:9091"
    environment:
      COORD_NODE_ID: "coord-node-1"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_CLUSTER_PEERS: "coord-2:9090,coord-3:9090"
      COORD_BOOTSTRAP: "true"
      COORD_DATA_DIR: "/data"
    volumes:
      - coord-data-1:/data
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 12
      start_period: 10s

  coord-2:
    image: nexus.byteforce.cn/image-private/coord:${COORD_VERSION:-0.1.14}
    container_name: coord-2
    hostname: coord-2
    command: ["server"]
    restart: unless-stopped
    networks:
      - coord-net
    ports:
      - "19090:9090"
      - "19091:9091"
    environment:
      COORD_NODE_ID: "coord-node-2"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_CLUSTER_PEERS: ""
      COORD_BOOTSTRAP: "false"
      COORD_DATA_DIR: "/data"
    volumes:
      - coord-data-2:/data
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    depends_on:
      coord-1:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 12

  coord-3:
    image: nexus.byteforce.cn/image-private/coord:${COORD_VERSION:-0.1.14}
    container_name: coord-3
    hostname: coord-3
    command: ["server"]
    restart: unless-stopped
    networks:
      - coord-net
    ports:
      - "29090:9090"
      - "29091:9091"
    environment:
      COORD_NODE_ID: "coord-node-3"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_CLUSTER_PEERS: ""
      COORD_BOOTSTRAP: "false"
      COORD_DATA_DIR: "/data"
    volumes:
      - coord-data-3:/data
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    depends_on:
      coord-1:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 12

networks:
  coord-net:
    driver: bridge

volumes:
  coord-data-1:
  coord-data-2:
  coord-data-3:
YAML

docker compose up -d
docker compose ps
```

上述示例同时启用了 Docker `json-file` 日志轮转，单容器默认上限为 `10m x 3`。

指定镜像版本（默认已对齐 `0.1.14`）：

```bash
COORD_VERSION=0.1.14 docker compose up -d
```

### 节点端口映射

| 节点 | gRPC（宿主机） | HTTP（宿主机） |
|------|---------------|---------------|
| coord-1 | `9090` | `9091` |
| coord-2 | `19090` | `19091` |
| coord-3 | `29090` | `29091` |

---

## 二、集群自举流程

1. `coord-1` 以 `COORD_BOOTSTRAP=true` 启动，成为初始 Raft leader。
2. `coord-2` / `coord-3` 在 `coord-1` 健康后启动。
3. `coord-1` 根据 `COORD_CLUSTER_PEERS=coord-2:9090,coord-3:9090` 探测并将两个节点加入集群。
4. 三节点 quorum 建立后，集群进入正常服务状态。

### 验证集群就绪

```bash
curl http://localhost:9091/healthz
curl http://localhost:19091/healthz
curl http://localhost:29091/healthz

docker exec coord-1 coord ctl cluster status
```

---

## 三、安全域初始化（集群场景）

安全域初始化只需对当前 leader 执行一次（通常是 `coord-1`）：

```bash
docker exec coord-1 coord ctl operator init --shares-total 5 --threshold 3
```

保存 5 个 shares 和 root token，然后在每个节点上各提交 3 个 share 解封：

```bash
for NODE in coord-1 coord-2 coord-3; do
  docker exec "$NODE" coord ctl operator unseal shamir:AAAA...
  docker exec "$NODE" coord ctl operator unseal shamir:BBBB...
  docker exec "$NODE" coord ctl operator unseal shamir:CCCC...
done
```

> 集群重启后，每个节点都会单独处于 sealed 状态，因此需要对每个节点重复 unseal。

---

## 四、手动管理成员

```bash
# 查看当前成员
docker exec coord-1 coord ctl cluster status

# 新增节点
docker exec coord-1 coord ctl member add coord-4 coord-4:9090

# 移除节点（优雅下线）
docker exec coord-1 coord ctl member remove coord-4

# 移除不可达节点（强制）
docker exec coord-1 coord ctl member remove coord-4 --force-unreachable
```

---

## 五、追加 TLS / OTLP（可选）

如需在任一节点启用 TLS / mTLS 与 OTLP，请将以下环境变量和 volume 同样追加到对应服务：

```yaml
environment:
  COORD_TLS_CERT: "/certs/server.crt"
  COORD_TLS_KEY: "/certs/server.key"
  COORD_TLS_CLIENT_CA: "/certs/ca.crt"
  COORD_OTLP_ENDPOINT: "http://otel-collector:4317"
volumes:
  - coord-data-1:/data
  - ./certs:/certs:ro
```

---

## 六、备份与恢复

```bash
# 创建备份（快照写入容器临时目录）
docker exec coord-1 coord ctl backup create --file /tmp/coord-backup.json
docker cp coord-1:/tmp/coord-backup.json ./coord-backup.json

# 恢复（危险！会覆盖当前状态，操作前务必停服或确认数据）
docker cp ./coord-backup.json coord-1:/tmp/coord-backup.json
docker exec coord-1 coord ctl backup restore /tmp/coord-backup.json
```

---

## 七、停止并清理

```bash
# 保留数据卷
docker compose down

# 彻底清空（删除数据）
docker compose down -v
```

---

## 八、生产注意事项

- **Raft 容忍性**：3 节点可容忍 1 节点故障；5 节点可容忍 2 节点故障。
- **网络延迟**：节点间 RTT 建议 ≤ 10 ms，Raft tick 周期 100 ms。
- **数据目录**：使用块存储（SSD）挂载 `/data`，不要使用 NFS。
- **时钟同步**：所有节点 NTP 偏差应 ≤ 500 ms，否则影响锁 TTL 精度。
- **端口防火墙**：节点间 gRPC 端口（默认 9090）须互通；HTTP 控制面（9091）对外暴露时建议加 Nginx/mTLS 限制。
- **版本控制**：部署时使用显式镜像标签，例如 `0.1.14`，不要使用不确定标签。
