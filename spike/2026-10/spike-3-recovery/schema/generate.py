#!/usr/bin/env python3
"""生成 `com.china-workflow.*` 事件契约的 JSON Schema（每个事件类型一个文件）。

单一事实源就在本文件：事件类型清单 + 各自的 `data.status` 约束。手改生成出的
`*.json` 会在下次生成时被覆盖 —— 要改契约改这里。

用法：
    python3 schema/generate.py            # 写 schema/*.json
    python3 schema/generate.py --check    # 只校验已生成文件与清单一致（CI 用）
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parent
DATASCHEMA_PREFIX = "https://schemas.coord.dev/events"
SPEC_VERSION = "1.0"

# 引擎生命周期常量 → 对外契约类型 → 该事件发出时实例状态的允许集合。
# 状态取值由 `emit_lifecycle` 写入（`format!("{:?}", inst.status).to_lowercase()`）。
EVENT_TYPES: dict[str, list[str] | None] = {
    "com.china-workflow.workflow.started.v1": ["running"],
    "com.china-workflow.workflow.waiting.v1": ["waiting"],
    "com.china-workflow.workflow.suspended.v1": ["suspended"],
    "com.china-workflow.workflow.resumed.v1": ["running"],
    "com.china-workflow.workflow.completed.v1": ["completed"],
    "com.china-workflow.workflow.faulted.v1": ["failed"],
    "com.china-workflow.workflow.cancelled.v1": ["cancelled"],
    # 任务级事件发生在实例可能处于 running/waiting 等多种相位时，不逐型固定。
    "com.china-workflow.task.started.v1": None,
    "com.china-workflow.task.completed.v1": None,
    "com.china-workflow.task.faulted.v1": None,
    "com.china-workflow.task.retried.v1": None,
}

ALL_STATUSES = [
    "pending",
    "running",
    "waiting",
    "suspended",
    "completed",
    "failed",
    "cancelled",
]


def data_schema(allowed_status: list[str] | None) -> dict:
    return {
        "type": "object",
        "description": "生命周期事件负载：实例状态快照（`emit_lifecycle` 的 data）",
        "required": ["workflowId", "workflowName", "workflowVersion", "namespace", "status"],
        "properties": {
            "workflowId": {"type": "string", "minLength": 1},
            "workflowName": {"type": "string"},
            "workflowVersion": {"type": "string"},
            "namespace": {"type": "string"},
            "status": {"enum": allowed_status or ALL_STATUSES},
            "output": {},
            "fault": {"type": ["object", "null"]},
        },
        "additionalProperties": False,
    }


def envelope(event_type: str, allowed_status: list[str] | None) -> dict:
    dataschema = f"{DATASCHEMA_PREFIX}/{event_type}.json"
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": dataschema,
        "title": event_type,
        "description": (
            "coord 工作流 Outbox 事件（投递面载荷 = OutboxRecord）。"
            "partitionkey = aggregate_id（= instance_id）；sequence 为 per-instance 单调序号。"
        ),
        "type": "object",
        "required": [
            "id",
            "aggregate_id",
            "sequence",
            "event_type",
            "specversion",
            "source",
            "dataschema",
            "time",
            "data",
            "created_at_ms",
            "status",
            "attempts",
            "next_attempt_at_ms",
        ],
        "properties": {
            "id": {"type": "string", "minLength": 1, "description": "全局唯一事件 ID（消费者去重键）"},
            "aggregate_id": {"type": "string", "minLength": 1, "description": "partitionkey = instance_id"},
            "sequence": {"type": "integer", "minimum": 1, "description": "per-instance 单调序号"},
            "event_type": {"const": event_type},
            "specversion": {"const": SPEC_VERSION},
            "source": {"type": "string"},
            "subject": {"type": "string"},
            "dataschema": {"const": dataschema},
            "time": {"type": "string", "format": "date-time"},
            "data": data_schema(allowed_status),
            "created_at_ms": {"type": "integer", "minimum": 0},
            "status": {"enum": ["pending", "delivered", "dead"]},
            "attempts": {"type": "integer", "minimum": 0},
            "next_attempt_at_ms": {"type": "integer", "minimum": 0},
            "delivered_at_ms": {"type": ["integer", "null"]},
        },
        "additionalProperties": False,
    }


def rendered() -> dict[str, str]:
    return {
        f"{event_type}.json": json.dumps(envelope(event_type, allowed), indent=2, ensure_ascii=False)
        + "\n"
        for event_type, allowed in sorted(EVENT_TYPES.items())
    }


def main() -> int:
    check = "--check" in sys.argv
    files = rendered()
    problems: list[str] = []
    for name, body in files.items():
        path = SCHEMA_DIR / name
        if check:
            if not path.exists():
                problems.append(f"missing: {name}")
            elif path.read_text(encoding="utf-8") != body:
                problems.append(f"stale: {name} (rerun schema/generate.py)")
        else:
            path.write_text(body, encoding="utf-8")
    if check:
        stale = sorted(p.name for p in SCHEMA_DIR.glob("com.china-workflow.*.json"))
        for name in stale:
            if name not in files:
                problems.append(f"orphan: {name}")
        if problems:
            print("\n".join(problems))
            return 1
        print(f"schema ok: {len(files)} files match the manifest")
        return 0
    print(f"wrote {len(files)} schema files to {SCHEMA_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
