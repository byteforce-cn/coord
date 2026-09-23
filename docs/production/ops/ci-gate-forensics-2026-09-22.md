# CI 门禁「常驻红 / 间歇红」定位（W2）

- **日期**：2026-09-22（**更新**：2026-09-23，第六轮见 §6）
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §2.2（红项表）、§3 D-04、§5 W2
- **体例**：与计划书一致 —— 每条结论必须给**可重跑/可复现**的取证路径。
  **区分「已定位」与「仅收窄范围」**：前者给根因，后者只给"不是哪一类"，不得含糊。
- **纪律**：按 §5 W2 的裁决，**W2 未完成 ⇒ 其余所有绿都不计入对外门禁证据**。
  因此本文只报告定位结果，不宣布任何门禁转绿。

---

## §0 方法：本环境没有 `gh`、job 日志要 admin，怎么取证的

计划书 §2.2 的限制是真实的：`/actions/runs/{id}/logs` 返回 **403**
（"Must have admin rights"，实测），本机也没有 `gh` CLI。本轮找到三条可用通道：

| # | 通道 | 端点 / 命令 | 拿到什么 |
|:--|:--|:--|:--|
| 1 | 匿名 REST（仓库 public） | `GET /repos/byteforce-cn/coord/actions/runs?per_page=40`、`GET /actions/runs/{id}/jobs` | run 列表、每个 job 的 `conclusion`、**每个 step 的 `conclusion`** |
| 2 | **check-run 注释**（本轮关键突破口） | `GET /repos/byteforce-cn/coord/check-runs/{job_check_run_id}/annotations` | job 内 `core.warning/error/setFailed` 产生的**分级注释文本** —— 日志拿不到，但**失败原因的文字**拿得到 |
| 3 | 第三方源码 / 数据 | `raw.githubusercontent.com`；`api.github.com/repos/RustSec/advisory-db/tarball/main` | action 的行为分支；通告库快照（可与 `Cargo.lock` 本地交叉比对） |

> **一个有用的巧合**：GitHub 把每个 job 也暴露为一个 **check run**，其 `id` 与
> `jobs[].id` **相同** ⇒ 用 `jobs` 接口拿到 id，再拿去 `/check-runs/{id}/annotations`
> 就能拿到该 job 的失败注释。这条路径不需要 admin。

---

## §1 W2-2：`cargo audit + deny`（定时红）—— ✅ **根因已定位**

### 1.1 现象（按 run 逐 job 聚合，非文档自述）

| run | 事件 | SHA | `cargo audit + deny` | `real-process chaos` | `weekly perf baseline` |
|:--|:--|:--|:--|:--|:--|
| 35522988906 | push | `8b65290` | **success** | success | skipped（仅 schedule） |
| 35554838318 | schedule | `8b65290` | **failure** | success | failure |
| 35560812972 | schedule | `8b65290` | **failure** | success | failure |
| 35567717479 | schedule | `8b65290` | **failure** | **failure** | failure |
| 35679949475 | schedule | `8b65290` | **failure** | success | failure |

**关键事实：同一个 SHA、同一份 workflow 文件，push 绿而 schedule 红。** 唯一变量是事件名。

### 1.2 根因（源码级，可复核）

`rustsec/audit-check@v2.0.0` 的 `src/main.ts` **结尾按事件名分叉**：

```ts
if (!shouldReport) { return; }

if (github.context.eventName == 'schedule') {
    // 定时：为每条通告**建 issue**
    await reporter.reportIssues(actionInput.token, advisories, warnings);
} else {
    // 其它：把结果写成 **check run**
    await reporter.reportCheck(actionInput.token, advisories, warnings);
}
```

而 `src/reporter.ts` 的 `reportIssues()` 对每条通告（漏洞与 warning 都算）调
`client.rest.issues.create({...})` —— 需要 **`issues: write`**。

`ci.yml` 的 `security-audit` job 当时只授了：

```yaml
permissions:
  contents: read
  checks: write     # ⇐ 只够 reportCheck()
```

⇒ 定时跑走到 `issues.create` 时 403，action 的 `main()` `catch` 里 `core.setFailed`。

### 1.3 四条相互独立的证据

| # | 证据 | 取值方式 |
|:--|:--|:--|
| ① | action 源码确有 `eventName == 'schedule'` 分支 | `curl -s https://raw.githubusercontent.com/rustsec/audit-check/v2.0.0/src/main.ts` |
| ② | **四条定时跑的 check-run 注释里都有** `failure: Resource not accessible by integration - https://docs.github.com/rest/issues/issues#create-an-issue`；而 push 跑（check-run `106110229877`）**没有**这一条 | `/check-runs/{id}/annotations` |
| ③ | 四条定时跑与 push 跑的 **audit 结果逐字相同**：都是 `1 warnings found!`（唯一一条是 bincode `RUSTSEC-2025-0141`，`unmaintained`、**非漏洞**、已在 `deny.toml` 显式豁免） | 同上（注释里的 `1 warnings found!` 是 `main.ts` 的 `core.warning(\`${warnings.length} warnings found!\`)`） |
| ④ | **不是"新通告"**：RustSec/advisory-db 在 push 跑（09-20T16:33Z 完成）与首个定时红跑（09-21T02:38Z）之间**没有任何提交**（`/repos/RustSec/advisory-db/commits` 显示上一提交停在 09-19T08:42Z，下一提交在 09-21T08:55Z —— **晚于**那次红跑） | API 提交列表 + 本地 `Cargo.lock` 交叉比对（`unzip` / `owned-alloc` / `ringbuf` 等新通告的 crate **均不在**本仓 `Cargo.lock`） |

⇒ 结论：**红的判据是"没有权限把结果写进 issue"，不是依赖有已知漏洞。**
这正是计划书 §7 规则 4 与 `remaining-known-gaps.md:249` 说的
「红色的门禁不是严格的门禁，是教人忽略的门禁」。

### 1.4 修复

`ci.yml` 的 `security-audit` job 补 `issues: write`（只给这一个 job），并把上述根因写进
job 注释。**不动任何判据、不降级任何门禁**。

### 1.5 两个必须写清楚的副作用与语义

1. **阻断语义原本就不在 step 7**：`reportIssues()`（schedule 分支）对漏洞**不会**
   `setFailed`（只有 push/PR 的 `reportCheck()` 会）。修好后，定时跑的**唯一阻断闸**是
   step 8 的 `cargo deny` —— 它以 `deny.toml` 的显式豁免表为准
   （`yanked = "deny"` / `unmaintained = "all"` / `unsound = "all"`），新增未豁免通告会让
   它 exit≠0。这比"修前"**更强**（见下）。
2. **step 8 此前从未执行过**：四条定时跑的 step 列表里，step 8 `cargo deny` 恒为
   `skipped`（step 7 一红就跳）。也就是说 `licenses` / `bans` / `sources` 三道
   **没有任何执行记录**。本地复跑（`cargo deny check bans licenses sources`）⇒
   `bans ok, licenses ok, sources ok` ⇒ 修好后它会真的跑并保持绿。

---

## §2 W2-1：`weekly perf baseline`（13/13 常驻红）—— 判定

### 2.1 现象收窄（不依赖日志）

该 job 只有一个会红的 step：`Run release benchmarks + regression gate`（`bash scripts/bench-ci.sh`），
注释里报 **`Process completed with exit code 101`**。

`scripts/bench-ci.sh` 开头是 `set -euo pipefail`，而第一条实质命令是**未加保护的**：

```bash
PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture >"$REPORT" 2>&1
```

⇒ **exit 101 = `cargo test` 本身失败**（Rust 测试失败/panic 的退出码），脚本在进入下面那段
Python 解析**之前**就退出了。

由此可以**排除一类常见推测**：`benchmark-results/baseline.json` 不在仓库里
（`git ls-files benchmark-results` 为空、`benchmark-results/` 目录不存在），
但"baseline 缺失/过期"那条路径的退出码是 **1 或 0**
（`if update or not baseline: 写入并 sys.exit(0)`）；
`missing metrics` 那条是 **1**。**都不是 101**。⇒ 红的原因在 `perf_bench` 测试内部。

### 2.2 复现（本地，release，与 CI 同参）—— ✅ **已复现，红因是测量方法而非产品劣化**

命令（与 `bench-ci.sh` 修前逐字一致）：

```bash
PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture
```

结果：**`test result: FAILED. 8 passed; 1 failed` / `EXIT=101`** —— 与 CI 的
`Process completed with exit code 101` 同形。失败点：

```
tests::bench_all panicked at coord/tests/perf_bench.rs:347:
PERF GATE (T5.21): 25 Region throughput 118 ops/s < 80% of single Region baseline 219 ops/s (ratio 0.541)
```

**同一次跑里有两组"同一段代码"的测量**，结论完全相反 —— 这就是根因的直接证据：

| 组 | 1 Region（基线） | 5 Region | 10 Region | 25 Region | 判据 |
|:--|--:|--:|--:|--:|:--|
| **单独实例**（`bench_multi_region_write_throughput`，与其他基准重叠少） | 35 ops/s | 44（1.245） | 43（1.216） | 48（**1.360**） | **PASSED** |
| **`bench_all` 实例**（与 `bench_raw_redb_*` / `bench_mvcc_write_*` / `bench_value_size_impact` **并发**） | **219** ops/s | 226（1.036） | 208（0.951） | 118（**0.541**） | **FAILED** |

两个关键读数：

1. **基线自身的漂移是 6 倍**（35 vs 219 ops/s）。同一份代码、同一个进程、相隔几分钟，
   只因并发负载不同。⇒ **0.80 这条判据落在噪声带宽里面**，它测的不是产品。
2. 25 Region 那格是 5000 次迭代（`iterations = num_regions * 200`），**是最长的一段**，
   因此它跨过的并发窗口最多 ⇒ 系统性地被压得最狠。

### 2.3 判据

**红 = 多个重型基准并发跑（默认 `test-threads = nproc`）争同一条 fsync 路径。**
不是产品劣化：同一提交、同一二进制，单独跑时 ratio 1.36。

### 2.4 修复（**不放宽阈值**）

`scripts/bench-ci.sh`：给 `cargo test` 加 `--test-threads=1`，把测量前提恢复为
"同一时刻只有一个基准在写盘"，并把上述证据写进脚本注释。

**未做（已记账）**：单 Region 基线只采样 200 次迭代、而 25 Region 是 5000 次 ——
样本量不对称是**仍然存在**的测量脆弱性。彻底修法是让各 Region 数使用**相同迭代数**并加预热；
本轮只消除了噪声源，**没有**动 0.80 阈值、**没有**删断言。

**验证**：见 §2.5。

### 2.5 串行复跑结果（✅ **已验证**）

命令（即 `bench-ci.sh` 修后的那条）：

```bash
PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture --test-threads=1
```

**`test result: ok. 9 passed; 0 failed` / `EXIT=0` / `finished in 324.22s`**

而且**两组测量都过**（同一次跑，两个实例各测一轮）：

| 实例 | 1 Region | 5 Region | 10 Region | 25 Region | 判据 |
|:--|--:|--:|--:|--:|:--|
| 单独 | 221 ops/s | 189（**0.859**） | 223（1.012） | 208（**0.944**） | PASSED |
| `bench_all` | 213 ops/s | 190（**0.893**） | 222（1.042） | 212（**1.044**） | PASSED |

两点值得记下：

1. **绝对值也不再被压低**：裸 Redb 写入从并发跑的 `57 ops/s` 回到串行的 `227–240 ops/s`。
2. **余量偏窄（最小 0.859）**：串行消除了噪声源，但 §2.4 记的"基线只采 200 迭代"这个
   不对称**仍在**。⇒ 本修复**降低了**假红概率，不宣称根治；若要台账级可靠，
   必须把各 Region 数改成**相同迭代数 + 预热**（已归入待办）。

---

## §3 W2-3：`real-process chaos`（14/31 间歇红）—— 范围**已收窄**（未定位根因）

### 3.1 拿到的新事实：失败**永远在第一个套件**

对 5 次跑逐 step 聚合（`/actions/runs/{id}/jobs`）：

| run | job 结论 | 失败 step | 其后 5 个 step |
|:--|:--|:--|:--|
| 35567717479 | failure | **step 7 `Run real-process chaos suites`**（`chaos_real` kill9 套件） | 全部 `skipped` |
| 35679949475 / 35560812972 / 35554838318 / 35522988906 | success | — | 正常执行 |

⇒ `14/31` 的残余分布**不是**"job 里 6 个套件随机各红一次"，而是
**「第一个套件（`chaos_real`，`kill9` 故障注入）随机红，一红就把后面 5 个套件全部跳过」**。
这解释了为什么这类红总是同一个 step。

### 3.2 明确**不**声称的东西

拿不到 job 日志（403）⇒ **不能**给出根因，也不能区分"产品缺陷/测试竞态/环境资源不足"。
以下两类说法都**不成立**，不得写入任何结论：

- ✗「chaos 的红是假红」——没有证据；
- ✗「chaos 的红是真缺陷」——同样没有证据。

### 3.3 可执行的下一步（判定流程）

按计划书 W2-3 的要求，产出的是**判定流程**而不是猜测：

1. **先扩证据面**：把 `chaos_real` 套件的失败信息落到**注释**里
   （在 `ci.yml` 的该 step 后加一步 `if: failure()` 的 `tail` 摘要到
   `$GITHUB_STEP_SUMMARY`）—— 这样下一次红**无需 admin 权限**即可读到失败用例名。
   这是**为了让门禁可诊断**，不是为了让门禁变绿。
2. **本地复跑同参**（`cargo test -p coord --test chaos_real -- --ignored --nocapture --test-threads=1`，
   跑前 `bash scripts/kill-stray-coord-procs.sh`）⇒ 若可复现，按 §7 规则 4 立项；
3. 若本地不可复现，按"环境/资源"归类并给出**可观测的判据**（例如记录
   `nproc` / 可用内存 / 残留进程数），而不是写"偶发"。

---

## §4 W2-4：分支保护（B5⑧）—— ⛔ 本环境**阻塞**

- 读该设置需要鉴权：`GET /repos/byteforce-cn/coord/branches/main/protection`
  ⇒ **401 Requires authentication**（匿名不可读）。
- 本环境没有 `gh`、也没有可用于 REST 的 token（推送走 VS Code 注入的
  `GIT_ASKPASS`，不应把凭据取出当 API token 用）。
- 因此 **W2-4 仍是未完成项**，且**无法由仓内产物证明**——与 §9 的 U-02（人力）同类：
  属**组织/仓库设置事实**，必须由有 admin 权限的人执行并留档。

可执行命令（交给有权限的人）：

```bash
# 需要 repo admin 的 token
gh api -X PUT repos/byteforce-cn/coord/branches/main/protection \
  -F required_status_checks.strict=true \
  -F 'required_status_checks.contexts[]=fmt + clippy -D warnings' \
  -F 'required_status_checks.contexts[]=workspace tests' \
  -F 'required_status_checks.contexts[]=proto contract (buf lint + breaking)' \
  -F enforce_admins=false -F required_pull_request_reviews='' -F restrictions=''
```

**验证判据**（设置后必须做）：向 `main` 直接 push 被拒。

---

## §5 与计划书的关系

| 计划书位置 | 本文的推进 |
|:--|:--|
| §2.2「定时门禁不可信」表 | 三处红各自**收窄/定位**：audit **根因已定位并修复**；perf **红在测试内部（已排除 baseline 那条路）**；chaos **收窄到第一个套件** |
| §2.2「限制与诚实声明」 | 原来"没有根因"的部分，靠 check-run 注释 + action 源码 + advisory-db 交叉比对补齐了一条取证通道（§0） |
| §5 W2-1 / W2-2 / W2-3 / W2-4 | W2-2 完成；W2-1 部分（见 §2.4）；W2-3 部分（判定流程见 §3.3）；W2-4 阻塞（§4） |
| §5 W2-5 负控制演练 | 第二轮已交付 `scripts/check-gate-drills.sh` + CI 接线；**九道门逐门负控制**（1.5 人日）仍未做 |

---

## §6 第六轮（2026-09-23）：把「取不到的日志」变成「失败的注解」

> 本轮的触发点是两次新的实跑，它们各带来一个**必须回答的问题**：
> ① schedule run `35810892940` 上 `workspace tests` 绿、而前一晚 push run
> `35745314425` 上同 SHA 红 ⇒ 间歇红的**测试名**是什么？② perf job 首次真跑即红
> ⇒ **红在哪条断言、什么数字**？两者都卡在同一个通道问题上（§0：日志 403、
> 工件 401、无 `gh`），所以本轮先解决通道，再谈结论。

### 6.1 两次实跑的读数（逐 job，可复现命令见 §0 表）

| run | 事件 | SHA | 关键 job 结论 |
|:--|:--|:--|:--|
| `35745314425` | push | `1a8c827` | `workspace tests` **failure**（step 8「Run workspace tests」，`exit 101`）；其余全绿（含 `cargo audit + deny`、`real-process chaos`） |
| `35810892940` | schedule | `1a8c827` | `workspace tests` **success**；`cargo audit + deny` **success**；`weekly perf baseline` **failure**（step 6，`exit 101`） |

**由 ① 直接闭环的**：W2-2 的修复（补 `issues: write`）在 **schedule 事件**下生效 ——
这正是第五轮 §2.2 要求"等定时跑"的那个分支，现已实证。

**由 ② 得到的时间取证**（`GET /actions/jobs/{id}` 的 step 时间戳）：

- perf 的失败 step 耗时 **7m49s**（02:34:53 → 02:42:42）。本机同参跑一次
  `cargo test --release ... --test-threads=1` 约 5.4 min ≈ CI 上"构建 + 跑完"的
  量级 ⇒ **不是编译失败、也不是超时**，而是 `perf_bench` 内部断言。
- 退出码 **101** 同时排除了 python 门禁路径（`REGRESSION`/`missing` 的退出码是 1，
  "报告无可解析指标"是 2）⇒ 红在 `perf_bench` 的 `PERF_GATE` 断言（或 release 编译），
  与 §2.1 的推断一致，本轮把它**从推断变成时间取证**。
- `workspace tests` 红跑耗时 **2m12s**、绿跑 **2m13s** ⇒ 整套件**跑完了**才失败，
  不是"早期崩溃/被杀"。`--no-fail-fast` 会列出全部失败用例 —— 但那文字在日志里，
  而日志取不到（这正是本轮要修的）。

### 6.2 落地①：**失败自描述**（门禁失败必须携带可读事实）

新增两个脚本（都进仓、都可本地跑）：

| 脚本 | 作用 |
|:--|:--|
| `scripts/ci-annotate-test-failures.sh <log>` | 从日志抽取：编译错误（含 `-->` 位置，**在错误行下方**）、`test ... FAILED` 用例名、`panicked at path:line:col` + 消息、`PERF GATE`/`REGRESSION` 行；按优先级发 **≤10 条** `::error::`（GitHub 每 step 上限）；同时写入 `$GITHUB_STEP_SUMMARY`。**恒 exit 0**（不得掩盖原始失败） |
| `scripts/ci-run-with-annotations.sh <log> <cmd...>` | 包装门禁命令：输出 `tee` 落盘 + 透传；失败时自动调用上面的注解器；**透传原退出码** |

接线（`.github/workflows/ci.yml`）：

- `workspace tests` job：两个测试 step（含 sim 套件）全部包装；
- `chaos-nightly` job：7 个真实进程套件 step 全部包装 + 新增
  **Gate 0 DoS/RSS drill** step（W4-5，见 §6.4）；
- `perf-bench` job：报告工件改为 `if: always()`（失败也上传）；`bench-ci.sh` 内部把
  cargo 输出从 `>"$REPORT"` 改为 `tee`，失败时对报告调注解器。

**为什么这是 W2 的核心而不是旁支**：注解是**匿名可取**的唯一通道（`GET /check-runs/{id}/annotations`）。
此前 perf 失败时输出被重定向进报告文件、报告工件又被 `skipped` ⇒ **一个数字都带不出来**；
`workspace tests` 的失败用例名同样永远带不出来。现在两者都会被以注解形式带出。

### 6.3 落地②：修掉 perf 的「静默通过」路径（自述式 no-op）

`bench-ci.sh` 的跨运行比较**在 CI 上不可能执行**：`benchmark-results/baseline.json`
不在仓库（`git ls-files benchmark-results` 为空），旧代码在"无基线"时
**静默**写入并把当前值当基线、`sys.exit(0)` —— 读起来像"PERF GATE PASSED"。

现在的行为：无基线 ⇒ 发 `::warning::` **明说"跨运行比较未执行"**，本轮只有
within-run `PERF_GATE` 硬闸；基线仅供本机/未来趋势用。

> **为什么不在仓里塞一份基线**：perf 指标全是 ops/s 与延迟 ⇒ 与**机器**强相关，
> 而 `ubuntu-latest` 是共享池。跨机 20% 比较会把硬件差异读成劣化 ——
> 这正是 perf 13/13 的历史教训。要把它变成真门禁，前提是**固定 runner**（未做，已记账）。

### 6.4 顺带：W4-5 第 2 项（DoS 的 RSS 峰值口径）落地

新增进程级用例 `coord/tests/dos_rss_peak_test.rs`（`#[ignore]`，已接 chaos job）：
spawn 真实 `coord server`（鉴权开启），发 100 × 8 MiB（累计 800 MiB，在飞并发 25）
的无凭据超大请求，要求**全部** `RESOURCE_EXHAUSTED`，并读**服务端子进程**
`/proc/<pid>/status` 的 `VmHWM`（峰值）断言有界。本机实测：

| 读数 | 值 |
|:--|--:|
| 被拒 | 100/100 |
| VmHWM 总增长 | 116 MiB（阈值 256 MiB） |
| 逐波增量 | +69.5 / +29 / +14 / +6.5 MiB（**递减** ⇒ 与 body 字节数无关） |

两个**测量口径**上的自我防护（第一版都踩过）：解析后若读到 `0` 必须**报错**
（否则解析 bug 会让阈值断言永远成立 ⇒ 门禁变 no-op）；另设"首波之后的漂移"断言，
专门抓"峰值随累计字节线性增长"这一无界读特征。

### 6.5 本轮仍未完成（需要下一次 CI 跑，或需要权限）

| 项 | 状态 | 等什么 |
|:--|:--|:--|
| perf 红的具体断言数字 | 🟡 通道已就位 | 下一次 schedule 跑（`cron: 17 2 * * *` 起 **每个** schedule 都带 perf job） |
| `workspace tests` 间歇红的测试名 | 🟡 通道已就位 | 下一次触发间歇红（本地复跑见计划书 §11 第六轮表） |
| W2-1 的"同迭代数 + 预热"测量修法 | ⏳ 待数字 | 上面两条的读数（不知道红在哪一格就改测量，属盲改） |
| W2-3 根因 | 🟡 收窄 | 最近两次 chaos 均 **success**（n=2，不足以宣称稳定化）；失败时的注解已接线 |
| W2-4 分支保护 | ⛔ 阻塞 | 仓库 admin（§4 已给命令与验证判据） |
| W2-5 九道门逐门负控制 | 🟡 **6/9 道已覆盖** | 既有 2 道（fmt、告警↔runbook）+ 本轮新增 4 道：`wire-descriptor`（字段号漂移）、`wire-sync`（rpc 改名）、`sdk-sync`（内部面 import 漂回）、**panic 路径**（注入非测试 `panic!`）；四道都先本地逐条验证“注入 ⇒ exit 1、还原 ⇒ 绿”，再写进 `gate-self-check` job。剩余 3 道的口径：P3–P6 需 lab/审计，P8 需仓库设置 + 真发布，P9 尚无机械门禁 |
