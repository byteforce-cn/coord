# 依赖治理与 bincode 退场记录（已完成）

> Owner: maintainers ｜ Last verified: 2026-11-20

- **现状**：bincode 已完全退场——依赖图无 bincode（`cargo tree -i bincode`
  无匹配）；持久化与 RPC 载荷统一为信封 V2（postcard，
  `MAGIC(4B) | VERSION_V2(1B)`），无前缀 / V1 / RPC codec=0 等旧格式一律**显式拒绝**
  （不静默错读、不试错解码）。当前唯一豁免条目为 `RUSTSEC-2024-0449`
  （unsafe-libyaml / serde_yaml，见 §4）。
- **目标（已达成）**：以分阶段、每步带判据的退场计划替换序列化格式并关闭豁免；
  后续依赖纪律 = 不得重新引入 bincode（bincode 无豁免，重新引入直接 fail-closed）。

---

## 1. 退场为什么分阶段（记录）

bincode 在本仓库不是工具库，而是**持久化格式**，直接承载多类**长期存活**的数据：

| # | 承载物 | 位置 | 退场后格式 |
|:--|:--|:--|:--|
| 1 | 快照 | `coord-server/src/storage/snapshot.rs` | 信封 V2（postcard；内部 `version=5`） |
| 2 | Raft 日志条目 | `coord-server/src/raft/log_store.rs`（Entry/Vote/Committed/LastPurged；region 模式复用同一实现） | 信封 V2 |
| 3 | auth 用户/角色/会话/吊销/bootstrap | `coord-server/src/auth/manager.rs`（`/_sys/auth/`） | 信封 V2（变体索引 = wire 的一部分，只允许末尾追加，见 `test_auth_op_variant_indices_appended`） |
| 4 | PD Region 元数据 | `coord-server/src/pd/meta_store.rs` | 信封 V2 |
| 5 | 对象存储 manifest | `coord-server/src/storage/object_store.rs` | 信封 V2 |
| 6 | `AppliedLogId` | `coord-server/src/storage/mvcc.rs` | `ALI1` 手写定长（无失败路径；旧 bincode 回退已删除） |
| 7 | raft RPC 载荷（跨节点） | `coord-server/src/raft/network.rs` | postcard（`payload_codec=1`；ADR-0007） |

⇒ 一次「换库」= **多处数据迁移 + 双向兼容窗口**；本仓当时无生产部署，按分级判据
逐阶段推进（见下一节表）完成。

---

## 2. 退场阶段（全部完成）

| 阶段 | 动作 | 判据（可执行） | 状态 |
|:--|:--|:--|:--|
| **P0 先立"格式可辨识"** | 给第 2/4/5 项加统一魔数 + 版本字节前缀 | 旧数据仍能解码；新数据带前缀；篡改前缀 ⇒ 显式报错 | ✅ |
| **P1 选型 + 双解** | 选型（ADR-0005：postcard，`VERSION_V2`）；读路径三路双解 + 精确消费 | 两种编码逐字节对照可往返；篡改矩阵全部显式失败 | ✅ |
| **P2 迁移窗口** | 写路径切 V2 + 四个直写面信封化（P2a 读先行 / P2b 写切换，ADR-0006）；RPC 载荷标记先读后写（R1/R2，ADR-0007） | 每面「旧行 / V1 / V2 / 混读 / 篡改」矩阵 + 写前缀白盒断言 + 负控制 | ✅ |
| **P3 关闭豁免** | 删除旧读腿与迁移阶梯；bincode 从依赖图消失；删 `deny.toml` 豁免 | `cargo deny check advisories` 无豁免；`grep bincode Cargo.toml */Cargo.toml` 与 `bincode::`（`.rs`）归零；全量测试绿 | ✅ |

> **完成记录**（2026-10-02）：P0–P3 落地，判据均以测试钉住——信封实现：
> `coord-server/src/storage/envelope.rs`（唯一 V2；旧格式显式拒绝）；各面拒绝用例
> （快照 / auth / SM 元数据 / PD 队列 / raft 四表 / PD Region / manifest / RPC）与
> 写路径白盒断言分布在对应模块测试中；负控制逐项实跑后还原。

**完成判据（已满足）**：`cargo deny check advisories` 无豁免；bincode 不在依赖图；
旧格式读写路径已删除（出现即为显式错误）。

---

## 3. 约定

- 格式演进只经**信封版本字节**（`storage/envelope.rs`）：新增字段/改结构属 Breaking，
  不得依赖 `#[serde(default)]` 之类的宽松解码行为。
- 篡改防护要求：每条读路径必须**精确消费**（拒绝尾随字节）且对未知版本/损坏前缀
  **显式失败**；新增持久化面必须复用统一信封。
- **不得**重新引入 bincode：`deny.toml` 无 bincode 豁免（`unmaintained = "all"`），
  重新引入将直接 fail-closed。

---

## 4. 豁免登记：RUSTSEC-2024-0449（unsafe-libyaml / serde_yaml）

- **通告性质**：`unsafe-libyaml 0.2.11` 为 **unmaintained**（非漏洞）；它是
  `serde_yaml 0.9.34+deprecated` 的 YAML 后端。`deny.toml` 的 `unmaintained = "all"`
  会拦下它（2026-10-12 通告库/工具口径更新后开始显式失败）。
- **为什么选择「记录后豁免」而不是立即升级/替换**：本仓 `coord-core` /
  `coord-agent` **直接依赖** `serde_yaml` 解析工作流 YAML
  （`coord-core/src/workflow/parser.rs`、`sw.rs`、`coord-agent/src/services/workflow.rs`）；
  `serde_yaml` 已 deprecated 且 `0.9.34` 为最后一版，没有同 API 的维护替代。换解析器
  （如 `serde-saphyr` / `saphyr`）是工作流 DSL 输入面的迁移，需独立评估 DSL 兼容性
  （锚点 / 多文档 / 流式等），不宜随依赖审计混做。
- **旁路**：`regorus 0.12+` 已把其 YAML 后端切到 `yaml_serde`；本仓固定
  `regorus 0.10.1`，升级可消除该侧路径，但无法消除本仓直连路径。
- **关闭路径**：完成工作流 YAML 解析迁移后，删除 `deny.toml` 的本条豁免并恢复
  `ignore = []`。
- **判据**：`cargo tree --target all -i unsafe-libyaml` 无匹配；
  `grep -rn serde_yaml --include=Cargo.toml .` 归零；`cargo deny check advisories`
  无豁免。

---

## 5. 通告处置记录（2026-10-13）

- **RUSTSEC-2026-0338 / 0339 / 0340 / 0341 / 0342（quinn-proto ≤ 0.11.15）**：以锁文件升级
  `quinn-proto 0.11.15 → 0.11.18` 修复（`quinn` 经 `reqwest` 进入依赖图，生产代码不直接
  使用，无源码改动）；`cargo audit` 复跑 0 vulnerabilities。修复 PR：#62。
- **RUSTSEC-2024-0449（unsafe-libyaml / serde_yaml，unmaintained）**：见 §4——登记豁免 +
  关闭路径（工作流 YAML 解析迁移）。登记 PR：#62。

---

## 6. 通告处置记录（2026-10-15）

- **RUSTSEC-2026-0346 / 0347 / 0348 / 0349（rustls < 0.23.46）**：`rustls 0.23.45 →
  0.23.46` 修复（经 `tonic` / `reqwest` / `hyper-rustls` 共用进入依赖图，无源码
  改动）；同期把仓库级 crates.io 镜像从 tuna 切到 rsproxy（tuna 未同步 0.23.46，
  解析阶段即失败）。修复 PR：#75。
- **RUSTSEC-2026-0353 … 0364（wasmtime 48.0.5）**：`wasmtime 48.0.5 → 48.0.6` 修复
  （cranelift / pulley 同版本簇一并升级）。wasmtime 是 `coord-agent`
  `plugin-wasm`（非默认 feature）的可选依赖，无源码改动；沙箱行为（fuel / epoch /
  内存上限 / 能力边界）由插件套件复验。修复 PR：#91。
- 两条均为**漏洞型**通告，由版本升级关闭；`deny.toml` 的 `ignore` 未因此新增条目。
  该结论**仅属于本次处置**——后续 unmaintained 豁免见 §7。

---

## 7. 豁免登记：RUSTSEC-2026-0543 / 0555 / 0559（chrono / chrono-tz / chrono-tz-build）

- **通告性质**：三条均为 **unmaintained（非漏洞）**。`chrono-tz-build` 是 `chrono-tz` 的
  build 依赖；`chrono` 经 `chrono-tz` 传递引入。本仓**不直接依赖**这三个 crate。
- **两条传递路径**（`cargo tree -i chrono-tz -e normal,build`）：

  | 路径 | 用途 |
  |:--|:--|
  | `openraft 0.10.0 → display-more 0.2.6 → chrono-tz 0.8.6` | 共识库的 `Display` 辅助（日志/状态展示的本地时间格式化） |
  | `regorus 0.10.1 → chrono-tz 0.10.4` | 策略引擎里时区/时间相关的内建函数 |

- **为什么不立即修**：
  - `openraft` 固定 0.10 稳定线（`Cargo.toml` 明写「升级须走升级演练」），`display-more`
    是它自己的依赖选择，本仓无法从外部摘掉；
  - `regorus` 升级会改变策略求值面的依赖与行为，属独立评估（与 §4 的 `regorus 0.12+`
    旁路同源）。两条都不宜随依赖审计混做。
- **关闭路径**：任一为真即删除对应豁免——
  1. `regorus` 升到 `0.12+`（其后端已切 `yaml_serde`，消除 `chrono-tz 0.10.4` 路径）；
  2. `openraft` 升级到不再引入 `display-more` / `chrono-tz` 的版本（消除 `0.8.6` 路径）。

  `chrono` 与 `chrono-tz-build` 的豁免随之一起删除——它们只是 `chrono-tz` 的上下游。
- **判据**：`cargo tree -i chrono-tz -e normal,build` 无匹配；
  `cargo deny check advisories` 无豁免（`deny.toml` 的 `ignore` 回到只有 §4 一条）。
- **登记提交**：与 `deny.toml` 的 `ignore` 同一次提交；用
  `git log --oneline --grep 'chrono 族 unmaintained 豁免'` 定位。

---

## 8. 通告处置记录（2026-11-20）

- **RUSTSEC-2026-0567 / 0568 / 0569 / 0570 / 0571 / 0572 / 0573（wasmtime 48.0.8）**：
  8 条**漏洞型**通告（`Caller::get_module_export` panic、缺 `threads`/`gc` feature 时 abort、
  GC heap 基址失效、`AsyncWriteStream` / `wasmtime-wasi-http` 无界内存、组件模型 stream
  取消时 panic、GC heap 保留区非页对齐 abort）。`wasmtime 48.0.8 → 48.0.10` 修复
  （cranelift / pulley / wasmtime-internal-* 同版本簇一并升级）。wasmtime 是
  `coord-agent` `plugin-wasm`（非默认 feature）的可选依赖，无源码改动；沙箱行为
  （fuel / epoch / `StoreLimits` / 宿主 import 能力白名单）由插件套件复验。
- **为什么之前没拦住**：这三条依赖通告只在 `--all-features`（即启用 `plugin-wasm`）
  的依赖图里可见；CI 的 `cargo-deny-action` 用 `--all-features`，本地用默认特性跑
  `cargo deny check` 看不到它们。排查依赖通告必须以 CI 同口径的
  `cargo deny --all-features check` 为准。
- **RUSTSEC-2026-0543 / 0555 / 0559（chrono / chrono-tz / chrono-tz-build，unmaintained）**：
  见 §7——登记豁免 + 关闭路径。`unmaintained` 策略不变（仍是 `all`）。

---

## 9. 通告处置记录（2026-11-20，ADR-0019 密码学原语）

- **RUSTSEC-2023-0071（`rsa` 0.9.x，Marvin Attack）** 与
  **RUSTSEC-2026-0387（`crypto-bigint` 0.5.5，非恒定时间操作）**：ADR-0019 引入
  非对称签名时，`rsa 0.9` 与 `p256/p384/p521 0.13`（经 `elliptic-curve 0.13`
  使用 `crypto-bigint 0.5`）同时命中。处置是把 `rsa` 升到 0.10、ECC 三件套升到
  0.14（`elliptic-curve 0.14.3` → `crypto-bigint 0.7.11`）。两条都是**漏洞型**
  通告，靠版本升级关闭，`deny.toml` 未新增豁免。
- **升级代价**：`rsa 0.10` 删除 `pkcs1v15::SigningKey` / `pss::SigningKey` 包装，
  改用 `PrivateKey::pkcs1v15_sign_prehash` / `pss_sign_prehash`（验签对应
  `PublicKey::*_verify_prehash`）；`rsa 0.10` 的 digest/sha2 是 0.11，而工作区其余
  使用者固定在 0.10，因此以 `sha2_11` 别名并存——摘要字节仍由 0.10 计算，0.11 只
  作为 RSA 泛型参数的类型标记。
- **判据**：`cargo audit` 0 vulnerabilities；`cargo tree -i crypto-bigint@0.5.5`
  无匹配；`cargo deny --all-features check` 全绿；
  `coord-agent/tests/transit_interop_test.rs` 的 openssl 独立验签覆盖
  Ed25519 / ECDSA / RSA（PKCS#1 v1.5 + PSS，salt = digest 长度）。
