#!/usr/bin/env bash
# 审批事件契约门禁：`apis/contracts/schemas/events/` 与引擎事件目录（events.rs）一致。
#   1) catalog（`APPROVAL_EVENT_TYPES`）中每个事件类型恰有一个 schema 文件；
#   2) 每个 schema 的 `$id` == `https://schemas.coord.dev/events/<type>.json`（`dataschema_for` 产物）；
#   3) 每个 schema 是合法 JSON（`python3 -m json.tool`）；
#   4) 目录内不存在 catalog 之外的孤儿 schema（死契约面）。
# 类型清单用 grep 从 events.rs 提取；提取为空即失败（fail-closed，防止上游改名后
# 门禁静默空转成永远绿）。纯 bash + python3，不引入新依赖。
# 退出码：0 = 一致；1 = 存在缺口。
set -uo pipefail
cd "$(dirname "$0")/.."

EVENTS_RS="coord-core/src/workflow/approval/events.rs"
SCHEMA_DIR="apis/contracts/schemas/events"
PREFIX="https://schemas.coord.dev/events"
rc=0

# 只在 `APPROVAL_EVENT_TYPES = &[ .. ];` 数组体内抓事件类型字面量（不涉及其后的测试代码）。
types="$(awk '/APPROVAL_EVENT_TYPES/{f=1} f{print} f&&/\];/{exit}' "$EVENTS_RS" \
  | grep -oE '"com\.china-workflow\.[a-zA-Z0-9._-]+"' | sed 's/"//g')"

if [ -z "$types" ]; then
  echo "check-event-schemas: FAIL（未能从 $EVENTS_RS 提取事件类型清单——门禁拒绝空转）" >&2
  exit 1
fi

if [ ! -d "$SCHEMA_DIR" ]; then
  echo "check-event-schemas: FAIL（缺 schema 目录 $SCHEMA_DIR）" >&2
  exit 1
fi

count=0
while IFS= read -r type; do
  [ -n "$type" ] || continue
  count=$((count + 1))
  file="$SCHEMA_DIR/$type.json"
  if [ ! -f "$file" ]; then
    echo "MISSING: $file（catalog 类型无对应 schema）" >&2
    rc=1
    continue
  fi
  if ! python3 -m json.tool "$file" >/dev/null 2>&1; then
    echo "INVALID JSON: $file" >&2
    rc=1
    continue
  fi
  want="$PREFIX/$type.json"
  got="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("$id",""))' "$file")"
  if [ "$got" != "$want" ]; then
    echo "ID MISMATCH: $file（\$id=$got，期望 $want）" >&2
    rc=1
  fi
done <<< "$types"

while IFS= read -r path; do
  [ -n "$path" ] || continue
  base="$(basename "$path" .json)"
  if ! grep -qxF "$base" <<< "$types"; then
    echo "ORPHAN: $path（不在 $EVENTS_RS 目录清单内）" >&2
    rc=1
  fi
done < <(find "$SCHEMA_DIR" -maxdepth 1 -name 'com.china-workflow.*.json' | sort)

if [ "$rc" -ne 0 ]; then
  echo "check-event-schemas: FAIL（catalog $count 个类型，存在缺口）" >&2
  exit 1
fi
echo "check-event-schemas: OK（catalog $count 个类型，schema 与 $EVENTS_RS 一致）"
