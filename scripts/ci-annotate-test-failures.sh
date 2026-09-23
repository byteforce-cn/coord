#!/usr/bin/env bash
# 从 CI 失败日志里抽取「可执行的失败事实」，以 check-run 注解（`::error::`）回传。
#
# 为什么需要（2026-09-22/23，W2 门禁取证的直接结论）：
#   本仓的 **job 日志端点需 admin**（实测恒 403）、`gh` CLI 本机不存在、
#   **工件下载需鉴权**（实测 401）⇒ 匿名环境下唯一能把「哪条测试 / 什么断言 /
#   什么数字」带出 CI 的通道是 **check-run annotations**
#   （`GET /repos/{owner}/{repo}/check-runs/{job_id}/annotations` 匿名可取）。
#   本脚本把日志里的关键行转成 `::error::` 工作流命令 ⇒ 变成注解 ⇒ 可被匿名读取。
#   方法学见 `docs/production/ops/ci-gate-forensics-2026-09-22.md`。
#
# 用法：bash scripts/ci-annotate-test-failures.sh <logfile>
#   · 未给日志 / 文件不存在 ⇒ 打印提示并 **exit 0**（注解器自身绝不制造新失败）。
#   · 仅在 `GITHUB_ACTIONS=true` 下发出 `::error::`；本地运行时只打印摘要。
#   · GitHub 每 step 最多 10 条 error 注解 ⇒ 本脚本最多发 10 条（按优先级取）。
#
# 退出码：恒为 0（调用方负责保留原始失败退出码）。
set -uo pipefail

LOG="${1:-}"
if [ -z "$LOG" ] || [ ! -f "$LOG" ]; then
  echo "ci-annotate-test-failures: 无日志可注解（${LOG:-<empty>}）" >&2
  exit 0
fi

python3 - "$LOG" <<'PY'
import os
import re
import sys

log_path = sys.argv[1]
try:
    text = open(log_path, encoding="utf-8", errors="replace").read()
except OSError as exc:  # pragma: no cover - 防御
    print(f"ci-annotate-test-failures: 读取失败：{exc}")
    raise SystemExit(0)

lines = text.splitlines()

# 优先级（数字越小越先发）：0 编译错误 / 1 测试失败 / 2 panic / 3 性能门禁 / 9 兜底
items = []


def add(prio, msg, file=None, line=None):
    msg = " ".join(msg.split())  # 折叠空白（注解必须是单行）
    if not msg:
        return
    entry = (prio, msg, file, line)
    if entry not in items:
        items.append(entry)


# 1) cargo 编译错误（"error[E0599]: ..." / "error: ..."），位置在其下方 "--> path:line:col"
for idx, ln in enumerate(lines):
    m = re.match(r"^error(\[[A-Z0-9]+\])?: (.+)$", ln)
    if not m:
        continue
    file = line = None
    for nxt in lines[idx + 1 : idx + 3]:
        pm = re.match(r"^\s*-->\s+([^:]+):(\d+):(\d+)\s*$", nxt)
        if pm:
            file, line = pm.group(1), int(pm.group(2))
            break
    add(0, f"编译错误: {m.group(0)}", file, line)

# 2) libtest 失败行（cargo test 的 "failures:" 段与逐行 FAILED 标记）
for ln in lines:
    m = re.match(r"^test (\S+) \.\.\. FAILED\s*$", ln)
    if m:
        add(1, f"测试失败: {m.group(1)}")

# 3) panic 位置与消息（Rust 1.98 形态："thread 'x' panicked at path:line:col:"）
for idx, ln in enumerate(lines):
    m = re.search(r"panicked at ([^:]+):(\d+):(\d+):?$", ln)
    if not m:
        continue
    msg = ""
    for nxt in lines[idx + 1 : idx + 4]:
        if nxt.strip():
            msg = nxt.strip()
            break
    add(2, f"panic: {msg or ln.strip()}", m.group(1), int(m.group(2)))

# 4) 性能门禁 / 劣化行（bench-ci.sh 的口径）
for ln in lines:
    if re.search(r"PERF GATE|REGRESSION |PERF GATE FAILED|assertion", ln):
        add(3, ln.strip())

# 5) 兜底：什么都没抽到 ⇒ 发最后 5 行非空输出（避免"注解为空"）
if not items:
    tail = [ln.strip() for ln in lines if ln.strip()][-5:]
    for ln in tail:
        add(9, f"日志尾部: {ln}")

on_gha = os.environ.get("GITHUB_ACTIONS") == "true"


def esc_data(s):
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def esc_prop(s):
    return esc_data(s).replace(":", "%3A").replace(",", "%2C")


items.sort(key=lambda e: e[0])
selected = items[:10]

print(f"ci-annotate-test-failures: 抽取 {len(items)} 条，发出 {len(selected)} 条"
      f"（日志：{log_path}）")
for prio, msg, file, line in selected:
    print(f"  - [{prio}] {msg}" + (f"  ({file}:{line})" if file else ""))
    if on_gha:
        if file and line:
            print(f"::error file={esc_prop(file)},line={line}::{esc_data(msg)}")
        else:
            print(f"::error::{esc_data(msg)}")

extra = len(items) - len(selected)
if extra > 0:
    print(f"（因每 step 10 条注解上限，另有 {extra} 条未发出；完整日志见工件/本地 {log_path}）")

summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    try:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("## 失败摘要（ci-annotate-test-failures）\n\n")
            for prio, msg, file, line in selected:
                loc = f" — `{file}:{line}`" if file else ""
                fh.write(f"- {msg}{loc}\n")
            if extra > 0:
                fh.write(f"\n另有 {extra} 条未发出（10 条上限）。\n")
    except OSError:
        pass
PY
