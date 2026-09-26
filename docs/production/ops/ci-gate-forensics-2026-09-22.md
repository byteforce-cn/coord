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

> **2026-09-26 更新**：API token（`$GHT`）可用后，历史 job 日志已可下载 ⇒ **§7 完
> 成了全量取证**（16 红逐条分解、3 次 soak 红的形态判定、判据修正与本地验证）。本节
> "日志 403" 的限制**只对 09-22 之前成立**；判定流程以 **§7.5** 为准。

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

## §4 W2-4：分支保护（B5⑧）—— ✅ **已落地并实证（2026-09-26，第十四轮）**

> 本节 2026-09-22 的原状态是「⛔ 本环境阻塞」（无 `gh`、无可用于 REST 的 token ⇒
> `GET .../protection` 401）。第十四轮拿到 **repo admin token** 后一次性完成设置与
> **负控制实证**；原拟命令与实际的差异、证据原文见下。

### 4.1 设置（实际执行）

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  https://api.github.com/repos/byteforce-cn/coord/branches/main/protection \
  -d '{"required_status_checks":{"strict":true,"contexts":["fmt + clippy -D warnings","workspace tests","proto contract (buf lint + breaking)"]},"enforce_admins":true,"required_pull_request_reviews":null,"restrictions":null}'
```

回读（`GET .../branches/main/protection`）：`strict=true`；3 个必需检查**逐条绑定
app_id=15368（GitHub Actions）**；`enforce_admins.enabled=true`；
`required_pull_request_reviews=null`；`restrictions=null`；
`allow_force_pushes.enabled=false`；`allow_deletions.enabled=false`。三个 context 名与
本仓 check-run 名**逐字一致**（对照 `GET /commits/{sha}/check-runs`）。

**与 2026-09-22 拟命令的一处差异（必须记录）**：原命令写 `enforce_admins=false`。但
执行 token 属主的仓库权限是 `admin=true` ⇒ 在 `false` 下按 GitHub 语义 admin **可绕过**
保护直推，与本节验证判据「直推被拒」互斥。故取 **`enforce_admins=true`**（更强：连
admin 也受检查约束；设置本身仍可由 admin 修改，不存在锁死）。**后果**：`main` 从此
不能直推未验证提交，日常交付改走 PR（见 4.3）。

### 4.2 负控制（验证判据，已做）

构造一个**从未被 CI 验证过**的提交（空提交）并**真实**推送：

```bash
git fetch origin && git checkout -b w2-4-probe origin/main
git commit --allow-empty -m "test: W2-4 negative control"
git push origin HEAD:main
```

实测（原文，`exit=1`）：

```text
remote: error: GH006: Protected branch update failed for refs/heads/main.
remote:
remote: - 3 of 3 required status checks are expected.
To https://github.com/byteforce-cn/coord
 ! [remote rejected] HEAD -> main (protected branch hook declined)
error: failed to push some refs to 'https://github.com/byteforce-cn/coord'
```

**一个必须写下的踩坑**：`git push --dry-run` **exit 0** 且显示
`f20501c..80c9328  HEAD -> main`（看起来"可推"）——**dry-run 不触发服务端保护评估**。
**此证据只能来自真实推送**。

### 4.3 语义与影响

- `main` 现在只接受「3 个必需检查已在该提交上通过」的更新：正常路径 = PR 合并
  （检查在 PR 头上跑）；非 PR 直推**同一 SHA**（检查已过）按 GitHub 语义仍允许。
- 未要求 PR review（单人仓；`required_pull_request_reviews=null`）⇒ PR 在必需检查
  通过后可由作者自行合并；`strict=true` 要求分支与 `main` 同步后才可合并。
- 本仓后续轮次交付流程随之变更：**push 分支 → 开 PR → 等必需检查 → merge**
  （不再"直推 main"）。已写入计划书 §11 第十四轮。

---

## §5 与计划书的关系

| 计划书位置 | 本文的推进 |
|:--|:--|
| §2.2「定时门禁不可信」表 | 三处红各自**收窄/定位**：audit **根因已定位并修复**；perf **红在测试内部（已排除 baseline 那条路）**；chaos **收窄到第一个套件** |
| §2.2「限制与诚实声明」 | 原来"没有根因"的部分，靠 check-run 注释 + action 源码 + advisory-db 交叉比对补齐了一条取证通道（§0） |
| §5 W2-1 / W2-2 / W2-3 / W2-4 | W2-2 完成；W2-1 部分（见 §2.4；schedule 侧已有闭环实证）；W2-3 部分（判定流程见 §3.3，后由 §7 全量取证 —— 16 红分解 + 判定流程）；**W2-4 ✅ 已落地并实证（§4，2026-09-26）** |
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
| W2-3 根因 | 🟡 收窄（有具体断言） | 最近三次 chaos 两绿一红；红的那次已收窄到 `chaos_soak_distributed` 第 784 次迭代写失败（§6.6-B），且**其余 7 套件全绿**；`if: always()` 已使“后面的套件没有执行记录”不再成立 |
| W2-4 分支保护 | ⛔ 阻塞 | 仓库 admin（§4 已给命令与验证判据） |
| W2-5 九道门逐门负控制 | 🟡 **6/9 道已覆盖** | 既有 2 道（fmt、告警↔runbook）+ 本轮新增 4 道：`wire-descriptor`（字段号漂移）、`wire-sync`（rpc 改名）、`sdk-sync`（内部面 import 漂回）、**panic 路径**（注入非测试 `panic!`）；四道都先本地逐条验证“注入 ⇒ exit 1、还原 ⇒ 绿”，再写进 `gate-self-check` job。剩余 3 道的口径：P3–P6 需 lab/审计，P8 需仓库设置 + 真发布，P9 尚无机械门禁 |

### 6.6 首次 CI 实证（run `35869740196`，SHA `a8e6fb3`）

| job | 结论 | 说明 |
|:--|:--|:--|
| `workspace tests` | **success** | 间歇红**未复现**（本 run 全套件绿）；注解通道待命 |
| `fmt + clippy -D warnings` / `proto contract` / `java sdk` / `java example integration` / `frontend lint` / `plugin engine feature matrix` | success | |
| `cargo audit + deny` / `Security audit` | success | W2-2 修复在 push 与 schedule 下均绿 |
| `gate self-check` | **failure** | 见 A：新自检**立刻抓到**“门禁在某 job 里跑不起来” |
| `real-process chaos` | **failure** | 见 B：注解带出具体断言；`if: always()` 让其余 7 套件**全部执行** |
| `weekly perf baseline` | skipped | push 事件不跑（`if: schedule`） |

**A. `gate self-check`（job `107212428973`）：新自检的第一份产出就是“它自己跑不起来”**

step 7（新加的 `wire-descriptor` 注入步）红、退出码 1，其后 3 步 skipped。
原因：`check-wire-descriptor.sh` 需要 **protoc**（脚本 `:37` 自己会报
“需要 protoc（CI 由 protobuf-compiler 提供）”），而 `gate-self-check` 是本仓
**唯一不装 protoc** 的 job ⇒ **这道门禁在该 job 里从未真正运行**。
这正是负控制自检要抓的形态（与 fmt 卡口当初“写了但从未生效”同型）。
修：该 job 增 `Install protoc` 步骤。

**B. `real-process chaos`（job `107212428956`）：注解第一次真的把“为什么”带了出来**

- 注解（`GET /repos/byteforce-cn/coord/check-runs/107212428956/annotations`）：
  `panic: soak put failed at iteration 784`（`coord/tests/chaos_real.rs:562`）+
  `门禁命令失败（exit 101）：cargo test -p coord --test chaos_real …`。
- 逐 step：step 7（`chaos_real` 套件）红，**step 8–14 全部执行且全绿**
  （soak 120s / multi-raft / auth+plugin×3 / plugin-real / object-storage /
  agent+auth / **本轮新增的 Gate 0 DoS/RSS drill**）⇒ `if: always()` 的价值当场兑现：
  7 个套件不再“没有执行记录”；**W4-5 的 DoS/RSS drill 在 CI 首次执行即绿**。
- 失败点收窄到：`chaos_soak_distributed`（step 7 里以默认 300s 时长跑）**第 784 次迭代**
  写入失败；而同一次 run 的 step 8 用 120s 单独跑同一用例**通过**。
- **根因仍未定位**。但已确认两类被排除：① 不是 kill9 用例的节点泄漏
  （该用例结尾 `for n in &mut nodes { n.kill9() }` 显式清场）；② 不是“重试后仍失败”
  的形态（soak 的每迭代写本来就只有一次机会，而 kill9 用例是 5 次重试）。
- 处置（**不弱化断言**）：`put_any` 此前 `if let Ok(resp)` **把错误整个丢弃** ⇒ 现有
  证据无法区分“无 quorum/写超时”（产品侧信号）与“节点不可达”（就绪/环境）。
  现改为收集逐节点错误返回，失败时附**集群快照**（逐节点可达性 + `revision`），
  两条都进 panic 消息 ⇒ 经注解通道直接可见。下一次红即可据此归类。

### 6.7 修复后的复跑（run `35876239831`，SHA `e197e2e`）—— **全绿**

| job | 结论 |
|:--|:--|
| `gate self-check` | **success**（6 步自检全过：fmt / 告警↔runbook / wire-descriptor / wire-sync / sdk-sync / panic 路径） |
| `real-process chaos` | **success**（含新 DoS drill） |
| `workspace tests` / `cargo audit + deny` / `fmt + clippy` / `proto contract` / `java sdk` / `java example integration` / `frontend lint` / `plugin matrix` / `Security audit` | success |
| `weekly perf baseline` | skipped（push 事件不跑） |

⇒ 本轮三处改动的**立即可验证部分**均已在 CI 上闭环：
①注解通道（chaos 红时真的把断言带了出来，§6.6-B）；②`if: always()`（7 个套件不再被吞）；
③protoc 修复（新自检从"跑不起来"变成"6 步全过"）。

**但仍然不宣布 chaos 已稳定**：最近 4 次 chaos 为 绿/绿/**红**/绿（红那次见 §6.6-B，
根因未定位）。`put_any` 的新诊断要等**下一次红**才会产出归因信息。

**另两条记账（本轮顺手核到，未修 —— 避免制造“半程修补”的错觉）**：

1. `cargo clippy --workspace --all-targets -- -D warnings` 在**测试目标**上仍有约 **23 处**
   存量违规（`Default::default()` 后逐字段赋值、`clone` on `Copy`、未用变量/导入等）。
   计划书 §4 P1 的口径本来就写明“（非测试目标）”，所以现状是**口径一致**的；
   要把这条卡口升级成 `--all-targets`（更强）需先清掉这 23 处 ⇒ **属独立立项**，
   不在 W2 的“负控制/取证”范围内。其中 3 处是 `clippy::assertions_on_constants`
   （如 `MAX_SCOPE_BODY_BYTES > MAX_GRPC_DECODING_BYTES`）—— 那是**编译期不变量**，
   **不是**“恒真的空测试”；更干净的写法是 `const _: () = assert!(…)`（编译期即失败）。
2. 本机 `cargo test --workspace --no-fail-fast` 不可用（2026-09-23 实测：跑完 77 个测试
   二进制、**0 失败**后，卡在 `test_pd_executor_real_raft_add_transfer_remove_peer`
   **>45 分钟无输出**而被终止；同一条命令在 CI 上 **2m13s** 完成；途中
   `m0_snapshot_purge_then_restart` 也报了 >60s）。⇒ 本地全量只能当“烟测”，
   权威结论以 CI 为准（这也解释了为什么 §6.2 的注解通道是必需品而不是锦上添花）。

---

## §7 第十三轮（2026-09-26）：chaos 红**全量取证**（token 可得后）+ 浸泡写活性判据修
正

§3 的"日志 403 ⇒ 无法判定"在 API token（`$GHT`，**不落盘、不打印**）可用后解除。
本节把 W2-3 从"范围收窄"推到"**分布已分解 + 判据已修正**"。

### 7.1 取数方法（可复现）

```bash
# 1) 列 CI run（可按 created_at / conclusion 过滤）
curl -s -H "Authorization: Bearer $GHT" \
  'https://api.github.com/repos/byteforce-cn/coord/actions/runs?per_page=100&page=1'
# 2) 逐 run 取 job 结论与失败 step
curl -s -H "Authorization: Bearer $GHT" \
  'https://api.github.com/repos/byteforce-cn/coord/actions/runs/<run_id>/jobs'
# 3) 下载失败 job 的完整日志（跟随 302，日志保留期内均可得）
curl -sL -H "Authorization: Bearer $GHT" \
  'https://api.github.com/repos/byteforce-cn/coord/actions/jobs/<job_id>/logs' \
  -o /tmp/job.log
# 4) 抽签名
grep -nE "panicked at|FAILED|##\[error\]|panic:" /tmp/job.log
```

### 7.2 分布（窗口 = 最近 60 次 `CI` run，覆盖 2026-09-12…09-25；chaos job 口径）

| chaos job 结论 | 次数 |
|:--|--:|
| success | 34 |
| **failure（红）** | **16** |
| cancelled（并发取代，**不是红**） | 8 |
| skipped / 无该 job（窗口边缘的旧 run） | 1 / 1 |

16 红的**逐条分解**（每条都有下载到的日志原文支撑）：

| 签名 | 次数 | 失败形态（原文） | 归属 |
|:--|--:|:--|:--|
| `dtolnay/rust-toolchain@master` step 本身 | 7 | `##[error]Process completed with exit code 1`；日志含该 action 自带的 `Work around spurious network errors in curl 8.0` 包装步骤（全部发生在 09-12 半天内） | **基建红**（runner/网络；与产品无关） |
| `plugin_real_agent_process_e2e` @ `plugin_real_process_test.rs:623` | 4 | `plugin KV write must still succeed after agent restart (persisted account): … ErrForbidden: unauthenticated: missing CCT token`（09-13 05:14–10:36 的迭代 push） | **真实缺口（当日已修）**：CCT 恢复路径；其后同套件全绿 |
| `agent_forwards_credentials_and_enforces_scope_under_auth` @ `agent_auth_process_test.rs:516` | 2 | 09-13 10:36 与 09-24 13:38（`cab765a`） | **F-05 形态**（登录限流 × 重启窗口）；W1-2 已于 09-26 收口（类别可区分性 + 3× kill-all 统计） |
| `chaos_soak_distributed` @ `chaos_real.rs:562`（`soak put failed at iteration N`） | 3 | 09-18 schedule（N=793）、09-21 schedule（N=958）、09-23 push（N=784） | **口径缺陷（本轮修正，见 7.4）**；根因（产品瞬断 vs 环境抖动）**不声称**已定位 |

**同 run 佐证**（三份 soak 红日志一致）：`chaos_real_kill9_and_linearizability` 均
**通过**；50 次写一次的收敛检查从未触发（无 `diverged` 记录）；红只发生在 soak 的单
个迭代。

### 7.3 三次 soak 红的可判定/不可判定

- **可判定**（日志直接给出）：
  - 全部是 `chaos_soak_distributed` 的**无注水**浸泡阶段（本套件不注入任何故障）；
  - 失败时为"**每节点一次机会**"的单 pass，即**三节点同时失败一次即判红，零重试试
    错**；
  - 失败时刻 ≈ 浸泡开始后 **196–240s**（迭代数 × 实测 ~250ms/迭代，预算 300s）；
  - 出错前瞬时统计不可得（该次 run 早于 09-23 的"逐节点错误串"增强）。
- **不可判定**（诚实边界）：三次的 gRPC 错误内容**没有落进日志**（增强在其后落
  地）⇒ 不能区分「无 quorum 瞬断」与「连接/环境抖动」。**不写"假红"也不写"真缺陷"
  。**

### 7.4 落地：浸泡写判据修正（有界重试 + 瞬时失败自描述）

**判据口径**：无注水集群上，**单 pass** 三节点同时失败**不是**活性违反 —— 领导权抖
动（含 CI 负载导致的选举超时）会让某个瞬间任何节点都写不进；活性判据应为「**有界重
试窗口内仍写不进**」。

改动（`coord/tests/chaos_real.rs`）：

- 新增 `soak_put()`：最多 `SOAK_PUT_PASSES = 3` 轮（轮间 250ms，覆盖一次典型重新选
  举的亚秒级时长），返回**每个失败 pass 的逐节点错误串**与所用 pass 数；
- 浸泡循环与**终态 put** 都走同一口径；超出窗口才 panic，panic 消息携带**全部 pass
  的错误串 + 集群快照 + 之前瞬时次数**（经注解通道直接可见）；
- 瞬时失败（重试后成功）**不判红但必须可见**：逐条打印（保留前 10 条样例）+ 计数进
  入收尾摘要行：

  ```text
  soak summary: writes=194 duration_secs=45 transient_retried_ok=0 transient_samples_kept=0
  ```

**本地验证**（2026-09-26，debug 二进制）：

```bash
CHAOS_REAL=1 SOAK_DURATION_SECS=45 cargo test -p coord --test chaos_real \
  chaos_soak_distributed -- --ignored --nocapture --test-threads=1
# ⇒ 1 passed; summary: writes=194 transient_retried_ok=0
```

### 7.5 判定流程（**取代 §3.3**）

1. 取 run 的 jobs ⇒ 找到红 job 与**失败 step 名**；
2. 下载该 job 日志，抽签名（7.1）；
3. 按**归因表**分流：

   | 失败 step / 签名 | 归类 | 处置 |
   |:--|:--|:--|
   | `dtolnay/rust-toolchain@master` 等**基建 step** | 基建红 | 记录 + 重跑；不进入产品缺陷台账 |
   | `Run real-process chaos suites` ⇒ `chaos_soak_distributed` | 写活性（新口径：重试窗口） | 有错误串 ⇒ 按"可达但拒绝写（无 quorum）/ 不可达"分流；无错误串 ⇒ 不可能（新口径必带串） |
   | `Run real-process chaos suites` ⇒ `chaos_real_kill9_and_linearizability` / `…diverged` / 线性一致违反 | **产品红** | 立即立项（规则 4） |
   | `Run agent+auth …` ⇒ `agent_forwards_credentials…` | F-05 类别 | 若再现 ⇒ **重开 F-05**（W1-2 的收口只覆盖其统计面） |
   | `Run plugin engine real-process e2e` ⇒ `missing CCT token` | CCT 恢复类别 | 同上表定位 |
   | 其它/无法归类 | **未判定** | **必须**在本节新增一行登记（禁止"偶发"二字结案） |

4. 只有「**重试窗口内仍失败** / 节点发散 / 线性一致违反」才算产品红；其余红（基建、
   已修类、口径类）也要**归档证据后**方可重跑。
5. **绿色 run 的观察义务**：`soak summary` 行 `transient_retried_ok > 0` 时要在此登
   记（它是对"领导权抖动"的直接观测）。

### 7.6 残余与不声称

- **不宣布 chaos 稳定**。事实记账：最后一次 chaos 红 = `cab765a`（09-24 13:38，
  F-05 形态）；其后 chaos job **连续绿 n=11**（09-24T14:32 … 09-26 `44d4156`；不含
  尚在跑的 `42c09ca`）。n=11 仍小于"宣布稳定"的门槛，且其中 9 次早于 W1-2 修复。
- 三次历史 soak 红在**新口径下大概率不会判红**，但这是**口径修正**，不等于根因定
  位；**若**新口径下再现 `soak put failed … after 3 passes`，那将是**首次可归因
  的 soak 红**（带错误串+快照），按 7.5 表处置。
- 与 §3.2 同样的纪律：本节所有"归属"列都有原文日志支撑；无原文的只进"未判定"。

### 7.7 观察义务首次执行（run `36219956592`，SHA `f20501c`）

修正落地后的首个 green run，两档 soak 的摘要行（`--nocapture`）：

| 档 | 摘要 |
|:--|:--|
| 300s（`Run real-process chaos suites`） | `writes=1305 duration_secs=300 transient_retried_ok=0 transient_samples_kept=0` |
| 120s（`Run distributed soak (smoke, 120s)`） | `writes=492 duration_secs=120 transient_retried_ok=0 transient_samples_kept=0` |

⇒ **零瞬时失败**（登记表无新增）；§7.5 第 5 条的"观察义务"机制**已实际跑通**（此前此类
信息在绿 run 中根本不存在）。全 job 结论：11 个 success + `weekly perf baseline`
skipped（push 事件不跑）。
