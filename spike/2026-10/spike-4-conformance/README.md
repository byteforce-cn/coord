# Spike 4 · 规范符合性（CNCF Serverless Workflow 1.0.3）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 手册对应：§4 Spike 4（10-24 检查点初版）

## 产出

| 产出 | 路径 |
|:--|:--|
| 量化报告 | `REPORT.md` |
| 官方 Schema + 来源 | `schema/workflow-1.0.3.yaml`、`schema/PROVENANCE.md` |
| 语料（24 份） | `corpus/*.yaml` |
| 解析扫描工装（真实分发路径 + 语义摘要） | `harness/`（workspace 成员，仅 spike 分支） |
| 校验/合并脚本 | `scripts/schema_check.py`、`scripts/matrix.py` |
| 原始结果 | `evidence/parser-results.json`、`evidence/schema-results.json`、`evidence/matrix.tsv`、`evidence/summary.txt` |

## 复现

```bash
cd <repo-root>
S=spike/2026-10/spike-4-conformance
CARGO_TARGET_DIR=target cargo run -p spike-conformance -- "$S/corpus" > "$S/evidence/parser-results.json"
python3 "$S/scripts/schema_check.py" "$S/schema/workflow-1.0.3.yaml" "$S/corpus" > "$S/evidence/schema-results.json"
python3 "$S/scripts/matrix.py" "$S/evidence/schema-results.json" "$S/evidence/parser-results.json" "$S/evidence/matrix.tsv"
```

## 结果（2026-10-13）

- schema-合法语料 23 份：解析接受 19、硬拒 3、双方拒绝 1（另有 1 份 0.8 遗留
  格式被接受 = LAX）。
- 19 份「接受」中 6 份语义被静默误读/丢弃 → **语义可用 13/23**。
- 最严重：`run: script|container|shell|workflow` 全部编译为空引用子流程且不报错；
  `use.extensions` 静默丢弃。
- 详见 `REPORT.md` §3/§4。

## 与手册验收标准对照

| 标准 | 结果 |
|:--|:--|
| 量化差距清单 | ✅ `REPORT.md` §3/§4（支持 15 项 / 硬拒 3 项 / 行为不一致 8 项） |
| 官方 JSON Schema conformance 结果 | ✅ `evidence/schema-results.json`（Draft 2020-12，1.0.3） |
| extensions 支持测试 | ✅ 语料 17：接受但静默丢弃（非拒绝） |
| run:script/container/shell 支持测试 | ✅ 语料 12/13/14/18：接受但误读为空子流程 |
| 新解析器 vs 适配层建议 | ✅ `REPORT.md` §5（工作量/风险/影响 + 分阶段建议） |
