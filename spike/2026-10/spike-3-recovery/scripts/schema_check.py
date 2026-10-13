#!/usr/bin/env python3
"""用 `schema/*.json` 校验 kill -9 证据里每条 `com.china-workflow.*` 事件。

输入：`evidence/**/outbox.jsonl` 与 `evidence/**/events.jsonl`（harness 落盘的
原始投递载荷）。对每条带 `event_type` 的载荷：

- 必须有对应的 `schema/<event_type>.json`（缺 schema = fail）；
- 必须通过该 schema 的校验（jsonschema Draft 2020-12）。

用法：
    python3 spike/2026-10/spike-3-recovery/scripts/schema_check.py [evidence_dir]
退出码非 0 = 有 schema 缺失或校验失败（fail-closed）。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

SPIKE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_DIR = SPIKE_DIR / "schema"
DEFAULT_EVIDENCE = SPIKE_DIR / "evidence"


def load_schemas() -> dict[str, dict]:
    out = {}
    for path in sorted(SCHEMA_DIR.glob("com.china-workflow.*.json")):
        out[path.name[: -len(".json")]] = json.loads(path.read_text(encoding="utf-8"))
    return out


def iter_records(evidence: Path):
    for path in sorted(evidence.rglob("*.jsonl")):
        for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue  # 非 JSON 行（不应出现）不计入契约校验
            if isinstance(rec, dict) and isinstance(rec.get("event_type"), str):
                yield path, lineno, rec


def main() -> int:
    evidence = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_EVIDENCE
    schemas = load_schemas()
    if not schemas:
        print(f"no schemas under {SCHEMA_DIR}", file=sys.stderr)
        return 1

    checked = 0
    failures: list[str] = []
    seen_types: set[str] = set()

    for path, lineno, rec in iter_records(evidence):
        etype = rec["event_type"]
        if not etype.startswith("com.china-workflow."):
            continue  # 总线上的非契约事件（如测试触发的输入事件）
        seen_types.add(etype)
        schema = schemas.get(etype)
        if schema is None:
            failures.append(f"{path}:{lineno} no schema for {etype}")
            continue
        errors = sorted(
            Draft202012Validator(schema).iter_errors(rec), key=lambda e: list(e.path)
        )
        checked += 1
        for err in errors[:5]:
            failures.append(f"{path}:{lineno} {etype}: {list(err.path)} {err.message}")

    print(f"validated {checked} contract events; distinct types: {len(seen_types)}")
    for t in sorted(seen_types):
        print(f"  - {t}")
    if failures:
        print("FAILURES:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1
    print("schema_check: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
