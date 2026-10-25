# CI 墙钟优化设计（test / chaos / feature-matrix 三者并行化）

**Owner:** CI 维护者 · **Last verified:** 2026-10-24 · **状态:** 待评审

## 1. 意图与为什么

PR 的门禁反馈时间（墙钟）现在是 25 分 25 秒，全部由 `workspace tests` 的 `Build tests`
决定；它之后 `real-process chaos` 是 16 分 06 秒、`plugin engine feature matrix` 是 12 分 39 秒。
三者都长在同一个形状上：**一个 job 里串行做完一批彼此独立的工作**。所以墙钟还能再换一次
并行度，而不需要牺牲任何一条校验。

目标是把 PR/push 的墙钟压到 **12–15 分钟**，同时保证：

- 一条校验都不减少（尤其：新增集成测试文件不可能被漏跑）；
- branch protection 不动（required check 名字是契约）；
- 缓存条数不增加（仓库缓存已用 **8.5 GB / 10 GB**，驱逐一次就是 28 分钟冷编译）。

### 实测基线

数据来自 run 40111240166（PR，缓存命中）与 40113268920（push 到 main，缓存未命中）。

| job | 墙钟 | 构成 |
|:--|--:|:--|
| `workspace tests` | 25m25s | `Build tests` 19m09s + 跑测试 3m29s + sim 套件 1m37s |
| `chaos (kill9 + soak)` | 16m06s | build 6m22s + kill9 step 7m03s + soak 2m02s |
| `chaos (multi-raft + auth/…)` | 10m45s | build 7m55s + multi-raft 2m14s + 其余 ~12s |
| `plugin engine feature matrix` | 12m39s | 3 个 feature check 2m15s + 插件单测 3m33s + native-service 3m20s + component 2m17s + wasm 重建 28s |
| 其余 job | ≤ 6m | lint / gate self-check / java / proto / frontend / audit |

**`Build tests` 的成本结构**（同一缓存状态下对照）：只构建 1 个 test target 的 job 是 6m22s，
构建 8 个的是 7m55s ⇒ 依赖 + workspace 库的 base ≈ 6m，每个测试二进制的边际成本 ≈ 5–13s。
`--workspace --tests` 有 **152 个 test target**，边际合计 ≈ 13 分钟——这就是 19 分钟的来处。

**一次冷构建的逐 unit 账**（`cargo build --workspace --tests --timings`，4 并发，墙钟 20m35s，
累计 CPU 79.6 min；权重用来定分片盐）：

| 类别 | unit 数 | 累计 CPU | 占比 |
|:--|--:|--:|--:|
| workspace 集成测试二进制 | 150 | 52.2 min | 66% |
| workspace lib/bin（非测试构建） | 8 | 7.7 min | 10% |
| workspace lib 单测 harness | 7 | 7.1 min | 9% |
| 依赖 crate | 488 | 8.1 min | 10% |
| 依赖 build script（含运行） | 105 | 1.0 min | 1% |

⇒ “base”（依赖 + workspace 库，不含测试二进制）≈ 19.5 CPU-min，4 并发下就是实测的 ~6 分钟；
三片把 60 CPU-min 的测试二进制平摊后，每片 ≈ **20 CPU-min / ~5–6 分钟墙钟**。

`cargo test` 只占 3 分半，所以「只并行跑」没有意义：**必须并行构建**。并行构建的代价是每个
job 各付一次 base（≈6 分钟，依赖来自 `rust-cache`，workspace 库因源码变化必须重编）。

## 2. 目标 / 非目标 / 反目标

**目标**

1. PR/push 墙钟 ≤ 15 分钟（设计值 ≈13 分钟）。
2. 新增集成测试文件自动进入某个分片；漏跑必须让 required check 变红。
3. required check 名字不变：`workspace tests`、`real-process chaos (nightly + PR gate)`、
   `plugin engine feature matrix` 三个 context 仍由同名汇总 job 提供。

**非目标**

- 不改测试内容、不修 flaky、不合并测试二进制（需要改 `tests/` 布局，另立议题）。
- 不动 java / frontend / proto / audit job（均 ≤ 6 分钟）。
- 不改本地开发者的 profile 设置（`[profile.dev] opt-level = 1` 保持）。

**反目标**

- 为了快而跳过校验、把失败降级为 warning、或让某个分片「跑 0 个测试也算绿」。
- 让缓存条目数或体积上涨（一次驱逐 = 28 分钟冷编译，抵消全部收益）。
- 用 `|| true` / 重试掩盖间歇失败。

## 3. 约束

- 分支保护要求 check 名稳定（`gh api branches/main/protection` 里的 10 个 context）。
- GitHub 缓存上限 10 GB；当前 15 条、8.5 GB。`Swatinem/rust-cache` 对同一 key 的并发保存
  会追加 `-0/-1/-2` 后缀 ⇒ **多个 job 保存 = 多条缓存**。
- 公开仓库，分钟数不计费 ⇒ 可以接受「总 CPU 上升、墙钟下降」。
- 工具链 `rust-toolchain.toml` 固定 1.98.1；GHA runner 4 vCPU。
- 仓库门禁（AGENTS.md）：PR 前本机跑 fmt / clippy / check-panics / audit / deny / check-doc-refs。

## 4. 方案

### A. 单 job 内压 build（不分片）

`debug=0` + 剔除 spike harness + 保留单 job。改动最小，但上限是「19m → 14~16m」，
且墙钟仍绑在这一个 job 上，任何一个慢测试都直接进 PR 关键路径。**不足。**

### B. 只把 `workspace tests` 分片

收益 25m → ~17m（chaos 16m 接管关键路径）。**不足**：达不到 12–15 分钟。

### C. 三者一起并行化（已选）

- `workspace tests` → 3 个 matrix shard + 同名汇总 job；
- `chaos (kill9 + soak)` → 修掉重复跑 soak（现浪费 5 分钟）；
- `plugin engine feature matrix` → 2 个 job + 同名汇总 job。

### D. sccache（本轮不做）

sccache 缓存的是**单次编译**（rustc 产物），不缓存链接；而 rust-cache 已经让依赖命中，
剩下的 base 是「每次提交都变的 workspace 库」与「链接 152 个测试二进制」，两者 sccache
都帮不上，只会多出上传/下载与 4 vCPU 上的压缩开销。等出现**频繁冷缓存**（例如 runner
镜像或工具链升级导致 target 缓存反复失效）时再评估。

## 5. 设计细节

### 5.1 分片脚本 `scripts/ci-shard.sh`（新增）

分组不写进 YAML、不手写清单：**从 `cargo metadata --no-deps` 现算**，所以「新文件没被分到
任何一遍」在构造上不存在。手写清单才是会漂的东西。

- **单元（unit）= 一个会跑测试的 cargo target**：`<pkg>::lib`、`<pkg>::bin::<name>`、
  `<pkg>::test::<name>`（proc-macro 同样当 lib 处理）。当前 160 个默认单元
  （150 个集成测试 + 6 lib + 3 bin + 1 proc-macro）。
- **分配**：`shard = blake2b("<salt>:<unit>", 8 bytes) % N + 1`。hash 而不是轮询：新增一个
  单元只让**它自己**换片，已有单元的分组保持稳定（轮询会让后面全体移位）。salt 是脚本内常量
  （不读环境变量：分片归属必须对同一提交确定，否则重跑会在不同片上失败，反而更难定位）。
  salt 的选法：把每个分片构建的 `--timings` 报告（job 作为 artifact 上传）按 unit 取耗时当
  权重，对候选 salt 求「每片耗时和的最大偏差」取最小。**权重口径必须是 CI 自己的数据**：
  按源码字节数挑（71）偏 43%，按本机冷构建挑（279）在 CI 上偏 35%（不同机器的并发与链接
  串行化不同），用 CI 实测则 70 ⇒ 17.1 / 17.0 / 17.1 分钟（偏差 0.5%）。
- **分片数只有一处**：`N` 是脚本里的常量，由 `plan` 子命令输出给矩阵（见 5.2）。
  workflow 里不写第二份 `[1, 2, 3]`——否则「矩阵 3 条腿 / `selectors I/N` / `verify N`」
  三处会漂，漂了就是某个分片永远不跑而没人发现。
- **子命令**
  - `plan` → 输出 `{"matrix": {"shard": [1..N]}, "shards": N}`（供 `fromJSON` 与汇总 job 用）。
  - `groups --shard I/N` → 打印该分片的 cargo 选择符，**一行一组**：库单测一组
    （`--lib -p A -p B`）、集成测试/二进制单测一组（`--test X --test Y --bin Z`）。
    分两组是被 cargo 的口径逼的：`--lib` 是布尔开关，写两次直接报
    `the argument '--lib' cannot be used multiple times`；而同一条命令里一旦带了 `-p`，
    `--test NAME` 的匹配范围也会被这些 `-p` 收窄（target 名全局唯一，不需要收窄）。
  - `count --shard I/N` → 单元数（job 用它断言「非空」，避免分片静默跑 0 个）。
  - `verify --shards N` → **覆盖性门禁**（fail-closed）：
    1. 每个 `cargo metadata` 单元恰好落在一个分片；
    2. 每个分片非空；
    3. **workspace 成员的** `<manifest dir>/tests/*.rs`（一层）里每个文件都能对上某个 test
       target——抓 `autotests = false`、被 `required-features` 藏起来的文件、以及新加 crate 时的
       枚举漏项。扫描根目录只取 `cargo metadata` 的成员目录（`spike/…/spike-1-state-model/crate`
       不是成员，不能因为它变红）；
    4. `required-features` 的 target（`sim_jepsen_test` / `sim_chaos_test`）**必须有一条真的会执行
       它们的 ci.yml 命令**：门禁从 `.github/workflows/ci.yml` 里找含 `cargo test` 且含
       `--features sim-tests` 的行，断言每个 required-features target 名以 `--test NAME` 的形态
       出现在该行区域里。清单不放在脚本里自己对自己负责（那样删掉 step 也能绿）。
- `verify` 跑在**汇总 job**里（纯 bash + python3，无 Rust），这样「三个分片全绿」之外还多一层
  「合起来确实覆盖全部单元」的判据；它同时**自检**（见 6.3）：注入一个「某个单元不落任何分片」
  的补丁副本，断言 `verify` 必须变红——沿用 `gate self-check` 的口径（检查存在 ≠ 检查生效）。
- **「这一片真的在跑东西」的判据是单元非空，不是执行到的测试数**：`chaos_real`、
  `dos_rss_peak_test` 之类单元整体 `#[ignore]`，作为分片单元执行到的测试数是 0，这是既有语义
  （它们由 chaos job 带着环境变量跑），不由分片断言。分片断言的是「选择符非空」+「合起来 == 全部单元」。

### 5.2 job 拓扑

| 名称（context） | 变化 |
|:--|:--|
| `test plan`（新增，~10s） | 跑 `ci-shard.sh plan`，输出矩阵与分片数；无 Rust 依赖 |
| `workspace tests`（required，保留） | **改为汇总 job**：`needs: [test-plan, test-shard]`、`if: always()`，断言每个分片都成功，跑 `ci-shard.sh verify <N>` 与负控制注入 |
| `test shard I`（新增 matrix，来自 `needs.test-plan.outputs.matrix`） | 恢复共享缓存 → 装 protoc → `cargo build <selectors>` → `cargo test <selectors>`（带注解包装） |
| shard 1 额外 step | doc-test（`cargo test --workspace --doc`，~30s；分片已经把 crate 编好了）|
| `test-sim`（新增，~5m） | 两个 sim 套件独立成 job：`--features sim-tests` 会重编 coord-server，挂在分片上就是给关键路径 +2 分钟 |
| `chaos (kill9 + soak)`（保持不变） | kill9 step 加 `--skip chaos_soak_distributed`（现在 `--ignored` 把 soak 也跑了一遍，300s，随后又用 120s 重跑一次） |
| `plugin engine feature matrix`（required，保留） | **改为汇总 job** |
| `feature matrix: checks + unit tests`（新增） | 3 个 feature check + 插件引擎/metrics 单测 |
| `feature matrix: component + wasm`（新增） | native-service 3m20s + component 2m17s + wasm guest 重建 |

每个分片 job 保留现有语义：`Pre-clean leaked test processes`、失败自描述的注解包装
（`ci-run-with-annotations.sh`）、`fail-fast: false`（一个分片红不取消其它分片）。

### 5.3 缓存：每类构建只有一个 saver

- 测试面（`shared-key: tests`）：只有最轻的那个分片（salt=70 下是 `test shard 2`）设
  `save-if: ${{ matrix.shard == 2 }}`，保存的 ~20s 不落在关键路径上；
  `chaos-*` 改成 `save-if: false`（它们 Restore 的正是同一份 deps + workspace 库，自己的
  测试二进制重链 ≈ 13–20 秒）。
- feature-matrix 面（新增 `shared-key: plugin-matrix`）：只有 `checks + unit tests` 保存。
- 其余 job（lint / gate-self-check / java-example-it / security-audit / perf-bench）不动。
- 净效果：测试面从「test + 2 chaos 各存一份」变成**一份**，feature 面两份合一。
- **存量条目怎么退场**：旧 key 线（job 派生 key 的 `v0-rust-feature-matrix-*`、`v0-rust-tests-*`
  的多条后缀、以及早已退役的 `v0-rust-test-*`）不由代码删除——GitHub 对同 key 保存会追加后缀、
  超 10 GB 按 LRU 驱逐。所以 6.4 的口径是**稳态**（合并后的下一次绿跑 + 旧条目被驱逐），
  不是首次绿跑的瞬时值。若需立刻腾空，用 `gh api -X DELETE /repos/…/actions/caches/<id>`
  删指定条目（凭证仍只来自 `GH_TOKEN`），这是一次性运维动作，不进仓库脚本。

### 5.4 debug 信息等级（可选项，带测量口径）

CI 里给测试面设 `CARGO_PROFILE_DEV_DEBUG=line-tables-only`、`CARGO_PROFILE_TEST_DEBUG=line-tables-only`：

- 收益：codegen 与链接都不再产出完整 DWARF，测试二进制明显变小 ⇒ base 与每二进制成本下降，
  target 缓存变小（对 10 GB 上限友好）。
- 代价：失败时的 backtrace 有函数名 + 文件:行，但**没有局部变量**。panic 位置不受影响
  （来自 `panic!` 现场元数据，不是 DWARF）。
- 口径：只在**测试面的 job**里设（`test-shard` + `chaos-*` 必须一致，否则共享缓存互相失效）；
  首次落地用 `cargo build --timings` 的 JSON（上传为 artifact）核对：base 段与 test-target 段
  各自降 ≥10% 才保留，否则回退成现状。

### 5.5 不做的「顺手优化」

- 不把 `coord` / `coord-server` / `coord-agent` 的测试二进制合并：能省链接时间，但要改
  `tests/` 布局与 `common` 模块的可见性，属于另一个议题。
- 不降 `opt-level`（CI 里 `CARGO_PROFILE_*_OPT_LEVEL=0`）：编译更快、测试更慢，且改变
  load-sensitive 用例的时序特征——现在 CI 已经在为「负载导致的假红」付代价，不能朝反方向走。
- 不从 `--workspace` 里剔掉两个 spike harness：它们是 workspace 成员就是为了被编译校验，
  分片后每个只会落进一个分片。

## 6. 验收

1. 一次绿色 PR run 的三个 required context 都在 ≤ 15 分钟内结束，总墙钟 ≤ 15 分钟
   （基线 25m25s）。
2. `test shard *` 的单元数之和 = `verify` 报出的单元总数（当前 160），每个分片 ≥ 40 个单元。
3. **覆盖门禁的负控制（永久，不只是一次性证据）**：汇总 job 里先把真脚本跑一遍（必须绿），
   再把 `ci-shard.sh` 复制到 `/tmp` 并打一个「丢掉一个单元」的补丁，断言该副本**必须红**，
   最后确认原脚本仍绿。思路与 `gate self-check` 现有注入步骤一致。
4. 缓存：合并后**稳态**下 `gh api /repos/…/actions/cache/usage` 总量不高于现状（≤ 8.5 GB），
   测试面只有 1 条 `v0-rust-tests-*`（存量条目退场口径见 5.3）。
5. `chaos (kill9 + soak)` 的 kill9 step 日志里不再出现 `soak summary` 行（soak 只跑一次）。
6. 分片 job 的 `Build tests` step 用时 ≤ 12 分钟（缓存命中时），三个分片的最大值即 PR 关键路径。

## 7. 留给实现者的部分

- `ci-shard.sh` 的具体 hash 实现、`selectors` 的输出格式、与 `ci-run-with-annotations.sh` 的接线。
- 分片 1 里 doc-test 与 sim 套件的 step 顺序（sim 套件需要 `--features sim-tests`，会再编两个
  test target；放在 doc-test 之后，保证「先短后长」）。
- feature-matrix 两个 job 之间的 step 分配细节（按实测单个 step 的耗时微调）。
- 首次落地后按实测数据决定 5.4 是否保留，并把实测墙钟写回本文件的基线表。
