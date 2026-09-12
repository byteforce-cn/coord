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
| _（待生成）_ | soak / chaos / multi-raft / jepsen | 仍未入仓 |

> 生成方式：`bash scripts/collect-evidence.sh java-it`（起真实集群 → `mvn verify -Pit`
> → 落盘 run.log + MANIFEST + 校验和；退出码即 mvn 退出码）。
>
> soak / chaos / multi-raft / jepsen 的真实产物**仍未入仓** —— 这是**诚实状态**。
> Gate 3 出口要求至少一次真实 jepsen + soak 产物落盘；在产物出现前，README 中
> 不得声明「已通过长期运行验证」。
