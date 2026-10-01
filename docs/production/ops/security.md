# 安全模型与加固

> Owner: maintainers ｜ Last verified: 2026-10-01

Coord 的安全模型与关键加固项：默认 fail-closed，例外必须在配置中显式写明。
本文描述**当前行为**与可重跑的判据；运行操作见 `runbook.md`。

---

## 1. TLS fail-closed

### 1.1 行为

`auth_enabled = true` 且 gRPC bind 非 loopback 且未配置 gRPC TLS（`tls_cert/tls_key`）
⇒ **拒绝启动**（错误含 `R-SEC-04`，不静默降级明文）。唯一逃生阀 = 显式
`security.allow_plaintext_remote = true`（默认 `false`；启用时启动日志 WARN 明示）。
三处 fail-closed 判定（无鉴权 / gRPC 无 TLS / raft 无 mTLS+密钥）共用
`is_non_loopback_bind()` 单一实现（`coord/src/main.rs`）；dev 模式的 `allow_insecure`
必须显式给出。

### 1.2 测试判据（正反双向）

测试源：`coord/tests/plaintext_remote_failclosed_test.rs`（本机实测 4 ✓）。

| # | 形态 | 期望 | 实测 |
|:--|:--|:--|:--|
| 1 | 非 loopback gRPC + 鉴权 + 无 TLS + 无逃生阀 | 拒绝启动（exit≠0；信息含 `R-SEC-04`+`allow_plaintext_remote`；端口不残留） | ✅ |
| 2 | 同上 + `allow_plaintext_remote = true` | 启动到 serve 且日志 WARN 明示 | ✅ |
| 3 | 同上 + CA 签发 `tls_cert/tls_key/tls_ca` | 启动到 serve（raft TLS 启用时强制 `tls_ca`，沿用既有校验） | ✅ |
| 4 | loopback gRPC + 鉴权 + 无 TLS | 不因本规则拒绝（不误伤 dev/test） | ✅ |

复跑：`cargo test -p coord --test plaintext_remote_failclosed_test -- --test-threads=1`。
回归：`raft_sec03_failclosed_test` / `dev_insecure_bind_test` / `auth_enforcement_test` /
`cli_tls_test` 全绿；六道校验（wire-sync / wire-descriptor / sdk-sync / panics /
error-code / gate-drills）全 `exit 0`。

**配置同步**：README（EN/zh-CN）TLS 行、`config.example.toml` 注释、`boundaries.md`
B-SE-1、`runbook.md` 均描述上述 fail-closed 行为。

> **jepsen lab 现状**：lab 节点以显式 `security.allow_plaintext_remote = true` 运行
> （明文、非静默，仅限测试环境）；lab 启用 mTLS 是后续强化项。

---

## 2. KEK 供给（transit）

### 2.1 行为（fail-closed）

启动时**注入**密钥材料（不引入外部 KMS），"非外部 KMS"是显式声明的边界（见
`boundaries.md` B-SE-2）。

| 项 | 落地 | 可重跑判据 |
|:--|:--|:--|
| ① KEK 由注入材料派生（不是配置串） | `KEK = HKDF-SHA256(材料, info="coord-transit-kek-v1:" || kek_id)`；HMAC 密钥用同一材料、不同 info 域分隔 | `test_kek_comes_from_material_not_from_kek_id`（同 `kek_id`、不同材料 ⇒ 必须解不开；同材料 ⇒ 必须解得开）、`test_hmac_key_is_domain_separated_from_material` |
| ② **缺材料即拒绝启动**（fail-closed，不许静默降级） | `TransitKekMaterial::resolve`：`COORD_TRANSIT_KEK`（hex64）→ `<data_dir>/transit-kek.bin`（32B）→ **Err**。agent 侧 `serve()` 把该 Err **上抛**（fail-closed：不允许静默降级） | `test_resolve_without_any_material_is_fail_closed`（错误信息必须同时给出两条注入路径且含 `refusing to start`）、`coord-agent/src/lib.rs`(`services.transit = true` 分支的 `?`) |
| ③ 负控制测试 | 覆盖：长度 0/1/16/31/33/64 一律拒绝；空 hex / 仅空白 / 非 hex / 16 字节一律拒绝；**env 非法时不静默回落到文件**；文件长度不符必须报错（而不是当作"无材料"） | `test_kek_material_rejects_wrong_length`、`test_kek_material_from_hex_rejects_empty_and_bad`、`test_resolve_env_takes_precedence_and_does_not_fall_back`、`test_resolve_from_file_enforces_length`、集成层 `test_transit_without_injected_kek_material_is_fail_closed` |
| ④ 文档口径与实现一致 | `WHITEPAPER.md`、`boundaries.md` B-SE-2/B-SE-5/B-SE-6、本节、`runbook.md` | `grep -rn 'coord-transit-kek:' --include=*.md .` 的命中不得出现把旧公式当作**现状**的用法 |

### 2.2 运维形态（接入方/运维必读）

```bash
# 方式 A：环境变量（hex64 = 32 字节）
export COORD_TRANSIT_KEK=$(openssl rand -hex 32)
# 方式 B：密钥文件（32 字节原始材料，建议 0600；每个 agent 用自己的 data_dir）
head -c 32 /dev/urandom > /var/lib/coord-agent/transit-kek.bin && chmod 600 …
```

- **多 agent 必须共享同一材料**（否则一个 agent 写下的 DEK 另一个解不开——见 `boundaries.md` B-SE-6）。
- `transit` **默认关闭**；显式 `services.transit = true` 且注入材料为唯一可用形态。
- **仍不是外部 KMS**：材料落在 agent 主机上，主机被控即泄露（`boundaries.md` B-SE-2）。

### 2.3 当前状态

KEK 供给与 TLS fail-closed 均已落地（判据见 1.2 与本节）；**未经独立第三方安全审计**，
任何声明不得暗示已审计。剩余项：`cargo deny` 的 bincode 豁免替代路径
（见 `dependencies.md`）。

---

## 3. 回归检查（五条）

| # | 回归项 | 现状 | 判据 |
|:--|:--|:--|:--|
| 1 | 越权 | ✅ 既有测试（agent scope fail-closed、interceptor scope 拒绝） | `coord-agent` 的 `watch_scope_is_fail_closed_not_bypassed` 等 |
| 2 | DoS（含 RSS 断言） | ✅ RSS 峰值实测口径已接入 CI | `coord/tests/dos_rss_peak_test.rs`：100 × 8 MiB（累计 800 MiB，在飞并发 25）无凭据请求 ⇒ 全部 `RESOURCE_EXHAUSTED`，且**服务端子进程** `/proc/<pid>/status` 的 `VmHWM` 增长有界（本机实测：100/100 被拒；总增长 116 MiB，阈值 256 MiB）。两条自我防护：读数为 0 必须报错；另设"首波后漂移 ≤128 MiB"断言 |
| 3 | 空密钥启动失败 | ✅ `coord/src/main.rs:4019+` 的 `load_or_create_root_key` 测试段（「no key file may be generated when refusing」） | 单测 |
| 4 | 非 loopback raft 无密钥启动失败 | ✅ `coord/src/main.rs:2215-2221` + `coord/tests/raft_sec03_failclosed_test.rs`（负控制确认覆盖） | 进程级负控制 |
| 5 | 非可信字节解析鲁棒性（属性测试） | ✅ proptest 第一阶段：cache 值编解码往返/截断/过期、health 请求行解析、wire 解码任意字节不 panic | `cargo test -p coord-agent --lib prop_`；`cargo test -p coord-proto --test decode_proptest` |

**命令**（本地/CI 同参）：

```bash
cargo test -p coord --test dos_rss_peak_test -- --ignored --nocapture --test-threads=1
cargo test -p coord-agent --lib prop_            # 属性测试：cache 编解码 / health 请求行
cargo test -p coord-proto --test decode_proptest # 属性测试：wire 解码
```

**残余/边界**：① 只打 content-length 预检路径（第二条"带硬上限读取"由 `coord-server` 单测
`:1809`/`:1827` 覆盖）；② 阈值与漂移阈值为本机实测的 2.2×/2.6× 余量，换机器需重测
（测试会打印全部读数，证据归档时一并收录）；③ 客户端口连接数上限已落地（`network.max_connections`，默认 4096；超限立即断开，`/metrics` 可见活跃/拒绝计数）；残余：连接寿命无主动回收（`boundaries.md` B-CX-1）。

---

## 4. 依赖治理

`deny.toml` 的 bincode 豁免是**唯一**例外（`RUSTSEC-2025-0141`，unmaintained 非漏洞）；
「替换格式」以分阶段、有判据的迁移计划推进，见 `dependencies.md`。
