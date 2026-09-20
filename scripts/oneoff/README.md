# `scripts/oneoff/` —— 一次性迁移留档（v0.2.0）

本目录存放 **只执行一次、已执行完毕** 的迁移脚本与基线快照，仅为可复现性留档。
日常开发与 CI **不会**调用它们。

## 内容

| 文件 | 用途 |
|:---|:---|
| `agent_api.pre-v0.2.0.proto` | 迁移前 `coord-proto/src/proto/agent_api.proto` 的快照（`4a5e3ee`，1290 行单体，`package coord.agent`）。作为 wire 基线 |
| `split-agent-proto.py` | 把该单体按 service 块拆成 16 个 per-domain proto（`package coord.<domain>.v1`），并重写 `agent_api.proto` 只留 Handshake / Health / Replica |
| `verify-wire-split.py` | 把 `coord-proto/src/proto/*.proto` 合并解析后与基线快照比对：**消息 / 枚举 / service / rpc / 字段编号必须逐项不变** |

## 复现

```bash
# 仓库根
python3 scripts/oneoff/verify-wire-split.py
# → missing messages/services/enums 全为空、field diffs / rpc diffs 为空
# → `WIRE PRESERVED : True`，退出码 0
```

## 为什么保留

`contracts/v1.2.0` 的迁移约束是「**迁移 = 包名与文件位置的搬移，禁止趁机改 wire**」
（`apis/contracts/CHANGELOG.md`，`WHITEPAPER.md` §11）。这条约束若没有可复跑的判据，
就只是一句自述。本目录提供该判据的**基线与被验方两侧的完整材料**。

对应验收项：`docs/production/agent-ga-remediation-baseline.md` §6 的「wire 零变化（V4）」。
