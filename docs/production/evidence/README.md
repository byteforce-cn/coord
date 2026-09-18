# 生产验证证据（Production Evidence）

本目录**纳入版本控制**（`.gitignore` 显式放行），用于存放可复现、可审计的
生产验证产物。

> 背景（E4 整改）：此前 `.gitignore` 直接忽略整个 `docs/`，导致「声称验证过」
> 但仓库里拿不出任何证据。尽调/合规审查中，「无证据的验证声明」等同于未验证。

## 硬性要求

1. **一次真实运行 = 一份产物目录**：`docs/production/evidence/<UTC 时间戳>-<场景>/`
2. 每份产物至少包含：
   - `MANIFEST.md`：谁跑的、什么 commit、什么命令、什么环境、结论；
   - `run.log`：完整原始输出（不得截断、不得只贴结论）；
   - `sha256sums.txt`：产物自身校验和。
3. 产物必须是**真实执行**的输出。任何「未跑但写成通过」的记录视为造假。

## 如何生成

```bash
# 使用仓库脚本（自动落盘到本目录，含 MANIFEST 与校验和）
bash scripts/collect-evidence.sh soak-smoke      # 120s 分布式浸泡
bash scripts/collect-evidence.sh chaos           # kill9 / 分区 / SIGSTOP 循环
bash scripts/collect-evidence.sh multi-raft      # 3 节点 × 3 Region 进程级
bash scripts/collect-evidence.sh jepsen          # 真实 Jepsen（需 lein + 集群）
```

## 已有证据

| 目录 | 场景 | 状态 |
|:--|:--|:--|
| `20260912T112707Z-java-it/` | 真实 server + agent + Java 集成套件（48/48 通过） | ✅ 已入库 |
| `20260912T164636Z-round3-workspace-tests/` | 第三轮整改后的工作区全量测试（`passed=1909 failed=0`，提交 `8e2cb37`） | ✅ 已入库 |
| **Jepsen（`jepsen/` 测试套件，见下）** | 真实 3 节点 coord + 真实 nemesis | ✅ 已入库 |

### Jepsen 证据（`jepsen/docs/dev.md` 计划 m0–m1）

生成方式：`cd jepsen/lab && make test JEPSEN_PROVIDER=docker ...`，随后
`jepsen/scripts/collect-evidence.sh <label> store/coord/<时间戳目录>`（`store/coord/latest`
每次都重建，所以必须用显式时间戳目录归档）。

| 目录 | 场景 | 结论 |
|:--|:--|:--|
| `20260916T122844Z-baseline-partition-ring-60s/` | register + partition-ring 60s（基线） | 绿 |
| `20260916T132103Z-t1.4-idempotency/`、`…-seed42/` | T1.4 幂等专项，**零故障注入** | **红是结论**：F-01（Delete 无幂等 47/47、范围删重放 39/39 删掉新写入）、F-02（Put 命中丢 `prev_kv` 26/26） |
| `20260916T132137Z-m0-lab-verification/`、`…132139Z-jitter-before-f11-fix/`、`…132141Z-jitter-verification/` | M0 收口（env-reset STRICT_CLOCK、抖动修复前后对照） | 绿（抖动对照留作 F-11 复现凭证） |
| `20260916T150557Z-t1.4-regression-after-f01-f02-fix/` | coord 修完 F-01/F-02 后的同样 workload | **绿**：154 组 / 352 次重放 0 违反 |
| `20260916T150552Z-t1.4-cross-node-f03/` | `--nemesis kill --idem-replay-delay-ms 4000` | **红是结论**：13 组 `:revision-advanced` / `:version-over-advance` ⇒ F-03（去重缓存不跨节点）`confirmed-by-run` |
| `20260916T150614Z-t1.1-map-60s-kill/` | T1.1 map/delete + kill 60s | 绿（170 ops / 27 delete = 15.9%） |
| `20260916T150615Z-t1.2-txn-60s-kill/` | T1.2 txn 全形态 + kill 60s | 绿（176 txn：成功 141 / 失败 31，两条分支都跑到） |
| `20260916T150616Z-t1.3-scan-60s-kill/` | T1.3 scan/revision + kill 60s | 绿（93 scan / 44 read-at / 404 值观察） |
| `20260916T150632Z-cas-register-45s-first-real-txn/` | cas-register 45s | 绿（**首次真正发出 Txn**：此前 `txn-req` 构造报错，整条路径是假绿，见 F-13） |

### Jepsen 第四轮（2026-09-17：M1 收口 T1.5 + M2 起跑 T2.0/T2.1）

| 目录 | 场景 | 结论 |
|:--|:--|:--|
| `…-t1.5-mixture-120s-share/` | `--workload mixture` 120s rate 10（份额口径实测） | 绿：实测 op 份额 0.517/0.255/0.228（目标 .50/.25/.25）⇒ 钉住「份额 = 实例数占比」 |
| `…-t1.5-mixture-45s-kill/` | mixture + kill 45s（`make matrix-m1` 档） | 绿 |
| `…-f3-value-size-4k/` / `…-f3-value-size-64k/` | map + kill 60s，`--value-size 4096` / `65536` | 绿；`history.edn` 里值长度实测 4096 / 65536（确认大 value 真走上了路径） |
| `…-t1.5-mixture-soak-2h/` | mixture + soak 2h（rate 2 · seed 42 · `--checker soak` · `--map-min-deletes 200`） | 见 `PROGRESS.md` §1.4 |
| `…-t2.1-watch-45s-none/` | T2.0 冒烟：`--workload watch` + none 45s rate 5 | 绿：380 事件 / 91 会话（流建立 + 事件投递通路成立） |
| `…-t2.1-watch-120s-kill-f19/` | F-19 复现凭证：watch + kill 120s（未修前） | **红**：69 会话全 `:info`、0 事件（oneof 字段塞了普通 map）；**被 G6 + 样本门槛抓到，无假绿** |
| `…-t2.1-watch-120s-kill-f20/` | F-20 复现凭证 | **红**：6 条 `:watch-event-before-start`（op 的起点取自最后一次重开） |
| `…-t2.1-watch-120s-kill-f21/` | F-21 复现凭证 | **红**：7 条 `:watch-event-loss`（空会话被当成覆盖了 `(start, start+2]`） |
| `…-t2.1-watch-120s-kill/` | 修完 F-19/F-20/F-21 后的重跑（归档于 `20260917T153058Z`，第五轮补入） | **绿**：1070 事件 / 78 次流重开 / 0 违反（契约的「至少一次 + 续传 + 严格递增 + 无静默丢事件」在 kill 下成立） |

### Jepsen 第五轮（2026-09-17：T2.2 lease + T6.1 组合浸泡）

| 目录 | 场景 | 结论 |
|:--|:--|:--|
| `…-t2.2-lease-60s/` | `--workload lease --nemesis none --time-limit 60 --concurrency 2n`（seed 42） | 绿：`grants 207 / expiries 95 / keepalive 58 / revoke 54`（`keepalive-responses 348`），六类违反 0 |
| `…-t6.1-soakfull-90s-kill/` | `--workload soakfull --nemesis kill --time-limit 90 --concurrency 2n`（seed 42） | 绿：五面全跑到（map 134 / txn 57 / watch 54 / lease 30 / scan 12），`:unrouted 0`、`:insufficient []` |

> **证据效力（2026-09-18 更新）**：24 份带该字段的 Jepsen 归档已回填 §5.4 参数确认
> 存档链接（不可移动的 tag permalink → `PARAM-CONFIRMATION.md`），台账
> `共 26 份归档：待填 0 / 已回填 24 / 无该字段 2`。
> **但回填 ≠ 满签**：签回单里 **③（quiet 可用率 0.95 / 100 ops）未确认**
> （按 §5.4 需**引入方团队**签），因此**依赖 ③ 的门禁结论仍属内部参考等级、
> 不得用于引入评审**；另 2 份 2026-09-12 的旧归档（java-it /
> round3-workspace-tests）由另一套采集器生成、无该字段。
> 确认单与回填脚本：`PARAM-CONFIRMATION.md`（逐条结论 / 确认人 / 日期）、
> `jepsen/scripts/backfill-param-confirmation.sh`（`--check` 可查台账）。
> 另：这些 run 都发生在提交之前，MANIFEST 的「工作树」字段为 `DIRTY`。


> 生成方式：`bash scripts/collect-evidence.sh java-it`（起真实集群 → `mvn verify -Pit`
> → 落盘 run.log + MANIFEST + 校验和；退出码即 mvn 退出码）。
>
> **状态更新（2026-09-17）**：jepsen 侧已入仓 **26 份**产物（含 1 次 2h 浸泡、
> T2.2 lease 60s、T6.1 soakfull 90s，均由 `jepsen/scripts/collect-evidence.sh`
> 从真实 docker lab run 的 store 目录落盘）；chaos / multi-raft 的真实产物
> **仍未入仓** —— 这是**诚实状态**。
>
> Gate 3 出口要求至少一次真实 soak 产物落盘（jepsen 侧已满足）。但**覆盖面
> 与证据效力仍有两个缺口**：M3–M6 未执行（T6.1 的 72h 全比例因 lock/election/
> registry 属 M5 未实现而构造期硬失败），且归档证据尚未取得 §5.4 的书面参数
> 确认。因此本目录**不得**被引用为「已通过长期运行验证」；完整口径见
> `jepsen/docs/soak-closure-report.md` §0。
