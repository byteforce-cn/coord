# k8s 部署验证清单

> Owner: maintainers ｜ Last verified: 2026-10-01

- **对象**：`deploy/k8s/statefulset.yaml`（3 节点 StatefulSet + headless Service + PDB）
- **现状**：**本清单尚未在真实集群上跑完**。「静态核验」列来自**读清单 + 读代码**；
  「实机演练」列一律为 ⏳（尚未取得实机证据）。

---

## 1. 静态核验（逐条可校验）

| # | 检查项 | 结论 | 证据 / 锚点 |
|:--|:--|:--|:--|
| 1 | **就绪探针**用 `/ready`（未选主时 **503**） | ✅ 正确 | `/healthz` 是存活语义（永远 200，body 里才写 SERVING/NOT_SERVING，见 `coord-server/src/bff/mod.rs:183-195`）；`/ready` 在 `bff/mod.rs:197-212` 返回 503 |
| 2 | **存活探针**用 `/healthz`（不查 raft 就绪） | ✅ 正确 | 用 `/ready` 做存活会在选举期被 kubelet 重启，反而制造更多选举 |
| 3 | **镜像 tag** 与版本对齐 | ✅ `0.2.1` | 版本事实：`Cargo.toml:15` |
| 4 | **反亲和** | ✅ 软反亲和（`preferredDuringScheduling`，weight 100，`kubernetes.io/hostname`） | 硬反亲和在单节点 kind 集群上会 Pending ⇒ 不适合本地验证；软反亲和两全 |
| 5 | 优雅下线 | ✅ `terminationGracePeriodSeconds: 60` | 对应 `coord/src/main.rs` 的 SIGTERM → 先移交 leader 再退出 |
| 6 | PDB | ✅ `minAvailable: 2` | 保证驱逐时 quorum |
| 7 | 持久卷 | ✅ `volumeClaimTemplates` 10Gi（RWO）+ `data_dir=/var/lib/coord` | —— |
| 8 | 非 root / 最小权限 | ✅ `runAsNonRoot: true`、`runAsUser: 1000`、`fsGroup: 1000`、`allowPrivilegeEscalation: false`、`capabilities.drop: [ALL]` | —— |
| 9 | 配置渲染（bootstrap 仅 coord-0、其余 join headless 域名） | ✅ initContainer 用 `$HOSTNAME` 的序号 | `coord-0.coord:50051` |
| 10 | TLS 证书挂载 | ✅ `coord-tls` Secret → `/etc/coord/tls`（ro），配置里 `tls_cert/tls_key/tls_ca` | 生产化建议 cert-manager |
| 11 | 探针端口 | ✅ `port: http`（= `containerPort 50061` = grpc+10，与 BFF 监听口径一致） | `bff` 端口约定 grpc+10 |

> **注**：以上结论均为部署清单级核验，未改任何 Rust 代码；实机验证仍待做。

---

## 2. 实机演练（待做；每条给出可执行命令）

```bash
# 0. 前置
kubectl create secret generic coord-cluster \
  --from-literal=auth-root-key=$(openssl rand -hex 32) \
  --from-literal=raft-shared-secret=$(openssl rand -hex 16) \
  --from-literal=root-password='<strong pw>'
kubectl create secret generic coord-tls \
  --from-file=server.crt=server.crt --from-file=server.key=server.key --from-file=ca.crt=ca.crt

# 1. 拉起
kubectl apply -f deploy/k8s/statefulset.yaml
kubectl rollout status statefulset/coord --timeout=5m

# 2. 就绪探针（判据：3 个 pod 全 Ready，且 READY 列 2/2 之类的容器就绪）
kubectl get pods -l app=coord

# 3. leader 选举（判据：恰好 1 个 leader，member list 三成员 voter）
kubectl exec coord-0 -- coord member list --addr 127.0.0.1:50051

# 4. 优雅下线（判据：删 pod 后 60s 内退出，且期间集群仍有 leader）
kubectl delete pod coord-2

# 5. 持久卷（判据：重建后数据仍在 —— 写入 → 删 pod → 读回）
kubectl exec coord-0 -- coord server ...   # 或经 agent 写入
kubectl delete pod coord-1
kubectl exec coord-0 -- ...

# 6. PDB（判据：drain 一个节点时不会同时驱逐 2 个 pod）
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data

# 7. 反亲和（判据：多节点集群上 3 个 pod 尽量分散）
kubectl get pods -l app=coord -o wide
```

**演练产物**：`docs/production/ops/drills/<UTC 时间戳>-k8s-kind.md`，含上表每步的
实际输出与结论。

---

## 3. 已知缺口

| # | 缺口 | 说明 |
|:--|:--|:--|
| 1 | **无验证证据**：仓库里没有一次 k8s 端到端运行的归档 | 待实机演练补齐 |
| 2 | `image: byteforce/coord:0.2.1` 需要真实存在的镜像 | 依赖制品构建与镜像发布；镜像未发布前不存在 |
| 3 | 未验证**滚动升级**（N-1 兼容） | 见 `upgrade.md`（当前已写入「不支持」清单） |
| 4 | 未验证**备份恢复在 k8s 下的挂载路径** | runbook 的备份恢复用 `--data-dir`，需与 PVC 配置对齐 |
| 5 | mTLS 证书过期轮换流程未演练 | 并入 runbook 的密钥轮换演练 |
