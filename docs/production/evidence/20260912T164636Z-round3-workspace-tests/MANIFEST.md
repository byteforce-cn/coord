# Evidence MANIFEST — round3-workspace-tests

- **UTC**: `20260912T164636Z`
- **git commit**: `8e2cb37`（第三轮整改提交；工作树在该次运行时的差异 = 1 个文件，
  即本次新增的 `docs/production/design/remediation-round3-2026-09-12.md`）
- **rustc**: `rustc 1.98.1 (48a229cea 2026-09-01)`（与 `rust-toolchain.toml` 的
  `channel = "1.98.1"` 一致）/ `rustfmt 1.9.0-stable`
- **uname**: `Linux byteforce 6.8.0-139-generic #139-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug  1 03:52:05 UTC 2026 x86_64`
- **CPU**: 8 核
- **command**:
  ```bash
  bash scripts/kill-stray-coord-procs.sh      # 先清理孤儿测试进程（见下）
  cargo test --workspace --no-fail-fast > run.log 2>&1
  ```
- **exit code**: `0`

## 结论

本次运行**通过**：`passed=1909  failed=0  ignored=41`，**0 个 target 失败**
（`run.log` 中不存在 `test result: FAILED`，也不存在 `error: N targets failed`）。

## 为什么日志是 `run.log.gz` 而不是 `run.log`

本目录约定要求「完整原始输出，不得截断」。该次运行的原始日志为 **7.0 MiB /
17,637 行**，直接入库会让仓库体积显著膨胀。因此以 gzip 无损压缩存放，并**同时记录
压缩前后两份校验和**——任何人都可以解压后逐字节核对，信息没有任何损失：

```bash
gunzip -c run.log.gz | sha256sum
# 必须等于下面的 uncompressed_sha256
```

| 文件 | sha256 |
|:--|:--|
| `run.log.gz` | `3a0a033819db53d9799d9305766b31009787de912866b91c9c2631a1e15e666e` |
| `run.log`（解压后） | `21e8b72cf723da4b73c6a460727cee9f3b6c7a724211b7c10fd836e193b29cc7` |

## 与第三轮复核 §4.4 第 2 项的对应关系

评估方指出上一份入库证据（`20260912T112707Z-java-it/`）的两点问题：

1. **"证据与提交位脱钩"**：其 MANIFEST 记录的是 `e79f882` 且 `dirty files: 30`，
   无法对应到任一干净提交。本份证据记录的是**明确的提交 `8e2cb37`** 与
   `dirty files: 1`（且该 1 个文件就是本轮的报告正文，不是待验证的代码）。
2. **"不代表固定提交位的可复现产物"**：本份证据的运行命令、工具链版本与提交号
   均在 MANIFEST 中固定；同一提交 + 同一命令应可复现同一结论。

> 仍需承认的边界：本份证据是**单机 8 核**上的工作区全量测试，用于验证
> 「整改后的默认套件为绿」这一具体主张。它**不**是 Jepsen/soak 级别的系统级证据，
> 也不覆盖网络分区与时钟回拨下的选举/锁行为（见整改报告 §10）。

## 环境注意事项（复现前请先读）

该次运行前**先执行了** `scripts/kill-stray-coord-procs.sh`。原因：进程级测试套件
（`auth_enforcement_test` / `cluster_test` / `chaos_real` 等）在并行负载下超时中断时，
其 spawn 的 `coord server` 子进程会成为孤儿并持续抢占 CPU/端口，导致后续套件出现
"raft write timed out (no quorum?)" 这类**看起来像产品缺陷的假红**。

同一提交上的首次全量运行即命中该模式（`auth_enforcement_test` + `cluster_test` 两个
target 失败）；清理孤儿进程后单独重跑两者**均全绿**（3 passed / 12 passed）。
详见整改报告 §9.3。
