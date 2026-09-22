# 安全面现状与处置计划（W4-1 / W4-2 / W4-5）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W4、§3 D-11、P-Gate 6
- **纪律**：本文「现状」列的每条都带 `file:line` 或可重跑命令；「未做」的**必须**写清楚
  为什么不能在本轮做（尤其是会影响别的门禁的）。

---

## §1 TLS：当前**不是** fail-closed（W4-1，🔴 未完成）

### 1.1 事实（本轮核验）

`README.md:61` 自述：「**Not fail-closed**：`coord -- dev` / 鉴权开启且带
`raft_shared_secret` 的集群仍可**明文启动**。只有两种情况拒绝启动：
(a) 鉴权关闭 **且** bind 非 loopback；(b) raft 端口非 loopback 且既无 raft mTLS
又无 `raft_shared_secret`。」

代码锚点：
- (a) 在 `coord/src/main.rs:2062-2070`（`non_loopback` 判定 + `refusing to start with auth disabled on non-loopback bind`）。
- (b) 在 `coord/src/main.rs:2215-2221`（`raft_is_loopback` 判定 + `refusing to start: raft_addr=… is non-loopback with neither …`）。
- dev 模式的第三条在 `coord/src/main.rs:3789-3796`（`allow_insecure` 必须显式给）。

⇒ 也就是说：**「鉴权开启 + gRPC 端口在可路由地址上 + 无 gRPC TLS」这一条没有被拦**。
这正是 P-Gate 6 要关的口子。

### 1.2 为什么本轮**没有**直接改（关键约束）

**盲目加严会打断 P-Gate 3/4/5 的全部证据链。** 事实：

- jepsen lab 的节点配置是 `auth_enabled = true` + `raft_shared_secret`，**没有 TLS**
  （`jepsen/src/jepsen/coord/db.clj:132`、`:141`），而节点间地址是 **docker 网络 IP（非 loopback）**。
- 因此「非 loopback + 鉴权 + 无 TLS ⇒ 拒绝启动」这条一旦直接落地，
  **lab 里每个节点都起不来** ⇒ W3 的长跑、矩阵、soak 全部无法取证。

⇒ W4-1 **不是**一行改动，而是「TLS fail-closed **+ lab 启用 TLS**」的**联立变更**。
把它拆成两半做，任一半都会让另一边的门禁变红。

### 1.3 处置计划（待做，按顺序）

| 步 | 动作 | 判据 |
|:--|:--|:--|
| 1 | lab 侧启用 mTLS（`jepsen/lab` 生成/分发 cert，`db.clj` 配 `tls_cert/tls_key/tls_ca`） | lab 内 `make test WORKLOAD=map` 绿 |
| 2 | 新增拒绝规则：`auth_enabled = true` **且** gRPC bind 非 loopback **且** 无 `[tls]` ⇒ 拒绝启动 | 负控制测试：故意去掉 `[tls]` ⇒ 进程非 0 退出且**不降级明文**（断言：端口不可连） |
| 3 | 提供**显式**逃生阀（如 `security.allow_plaintext_remote = true`），并让 dev 模式与测试用它 | `grep` 出所有使用点；默认值为 `false`（fail-closed 默认） |
| 4 | 在 README（EN/zh-CN）与 `WHITEPAPER.md` 同步改口径（D-05/D-11 的一部分） | 四份文本逐条一致（P-Gate 9） |

**本轮交付**：本节的**现状 + 计划 + 阻塞理由**（可核验），**不是**「已完成」。
按 §7 证据规范，未做的不写成已做。

---

## §2 KEK 供给（W4-2 / U-04 / W4-2a，✅ 已实施 2026-09-22）

### 2.1 裁定与落地

**U-04（2026-09-21）裁定取 K2 + K3**：启动时**注入**密钥材料（不引入外部 KMS），
并把"非外部 KMS"写进边界清单。**W4-2a 于 2026-09-22 实施完毕**。

| 项 | 落地 | 可重跑判据 |
|:--|:--|:--|
| ① KEK **不再**由配置串确定性派生 | 修前：`SHA-256("coord-transit-kek:" || kek_id)`。修后：`HKDF-SHA256(材料, info="coord-transit-kek-v1:" || kek_id)`；HMAC 密钥用同一材料、不同 info 域分隔 | `test_kek_comes_from_material_not_from_kek_id`（同 `kek_id`、不同材料 ⇒ 必须解不开；同材料 ⇒ 必须解得开）、`test_hmac_key_is_domain_separated_from_material` |
| ② **缺材料即拒绝启动**（fail-closed，不许静默降级） | `TransitKekMaterial::resolve`：`COORD_TRANSIT_KEK`（hex64）→ `<data_dir>/transit-kek.bin`（32B）→ **Err**。agent 侧 `serve()` 把该 Err **上抛**（修前只是 `tracing::error!` 后少注册一个服务 = 静默降级） | `test_resolve_without_any_material_is_fail_closed`（错误信息必须同时给出两条注入路径且含 `refusing to start`）、`coord-agent/src/lib.rs`(`services.transit = true` 分支的 `?`) |
| ③ 负控制测试 | 覆盖：长度 0/1/16/31/33/64 一律拒绝；空 hex / 仅空白 / 非 hex / 16 字节一律拒绝；**env 非法时不静默回落到文件**；文件长度不符必须报错（而不是当作"无材料"） | `test_kek_material_rejects_wrong_length`、`test_kek_material_from_hex_rejects_empty_and_bad`、`test_resolve_env_takes_precedence_and_does_not_fall_back`、`test_resolve_from_file_enforces_length`、集成层 `test_transit_without_injected_kek_material_is_fail_closed` |
| ④ 文档口径同步 | `WHITEPAPER.md` §12.7 第 7 条、`boundaries.md` B-SE-2/B-SE-5/B-SE-6、本节、`runbook.md` §4.4 | `grep -rn 'coord-transit-kek:' --include=*.md` 归零（旧派生公式不得再出现） |

### 2.2 运维形态（接入方/运维必读）

```bash
# 方式 A：环境变量（hex64 = 32 字节）
export COORD_TRANSIT_KEK=$(openssl rand -hex 32)
# 方式 B：密钥文件（32 字节原始材料，建议 0600；每个 agent 用自己的 data_dir）
head -c 32 /dev/urandom > /var/lib/coord-agent/transit-kek.bin && chmod 600 …
```

- **多 agent 必须共享同一材料**（否则一个 agent 写下的 DEK 另一个解不开——见 `boundaries.md` B-SE-6）。
- `transit` 自 2026-09-22（U-11）起**默认关闭**；显式 `services.transit = true` 且注入材料为唯一可用形态。
- **仍不是外部 KMS**：材料落在 agent 主机上，主机被控即泄露（`boundaries.md` B-SE-2）。

### 2.3 与 P-Gate 6 的关系

P-Gate 6 的「KEK 供给裁定落地」项由此**转绿**（本地可重跑判据齐全）；
P-Gate 6 整体仍红——TLS fail-closed（W4-1）、第三方审计（W4-3）未完成，
且按 W2 的裁决，本轮所有绿在 W2 完成前**不计入对外门禁证据**。

---

## §3 Gate 0 四条回归（W4-5，🟡 部分）

| # | 回归项 | 现状 | 判据 |
|:--|:--|:--|:--|
| 1 | 越权 | ✅ 既有测试（agent scope fail-closed、interceptor scope 拒绝） | `coord-agent` 的 `watch_scope_is_fail_closed_not_bypassed` 等 |
| 2 | DoS（含 RSS 断言） | 🟡 有资源上限若干，但**无连接数上限**（`boundaries.md` B-CX-1）、无 RSS 峰值实测口径 | **待补**：RSS 断言口径 |
| 3 | 空密钥启动失败 | ✅ `coord/src/main.rs:4019+` 的 `load_or_create_root_key` 测试段（「no key file may be generated when refusing」） | 单测 |
| 4 | 非 loopback raft 无密钥启动失败 | ✅ `coord/src/main.rs:2215-2221` | 已实现；**需负控制测试**确认覆盖 |

**待补**：第 2 项的 RSS 峰值实测口径（属 W5/W3 的观测面，与 7×24 曲线共用采集）。

---

## §4 本轮的**已完成**项

| 项 | 内容 | 判据 |
|:--|:--|:--|
| W4-6 | `SECURITY.md` 支持版本表由「0.1.x」更新为「0.2.x 当前 / 0.1.x 已结束支持」，并写明「本表必须与 `Cargo.toml` 的 `workspace.package.version` 一致」 | 人工比对 `SECURITY.md` 与 `Cargo.toml:15`；`grep -n '0.1.x' SECURITY.md` 只剩「已结束支持」行 |
| W4-4 | bincode 豁免的**替代路径**立项（见 `dependencies.md`） | 该文档存在且每步带判据 |

---

## §5 依赖治理（W4-4）

见 `docs/production/ops/dependencies.md`。要点：`deny.toml` 的 bincode 豁免是
**唯一**例外（`RUSTSEC-2025-0141`，unmaintained 非漏洞），本轮把「替换格式」从
一句技术债升级为**分阶段、有判据的迁移计划**。
