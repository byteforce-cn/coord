#!/usr/bin/env bash
# 文档/注释引用可达性校验：
#   1) Markdown 链接 ](相对路径)：解析相对目标，必须存在
#      （跳过 http(s)/mailto/anchor/占位符）。
#   2) 正文 / 注释中的仓库路径 token 必须存在：前缀为
#      docs/ scripts/ jepsen/ apis/ deploy/ monitoring/；覆盖 md 正文与
#      代码扩展名（rs sh toml yml yaml xml java proto py）。
#      token 判定：最后一段含 "." 或 token 以 "/" 结尾（排除占位符与运行时产物目录）。
#   3) 警告（不置红）：Rust 注释中的日期（白名单文件除外）。
# 排除：构建/运行产物目录（target node_modules dist .git .ua .codegraph）与本清单文件。
# 退出码：0 = 全部可达；1 = 存在悬空引用。
set -uo pipefail
cd "$(dirname "$0")/.."

rc=0
python3 - <<'PY'
import os
import pathlib
import re
import sys

SKIP_DIRS = {"target", "node_modules", ".git", ".ua", ".codegraph", "dist", "__pycache__"}
# 一次性工作清单不属发布树（按后缀排除，避免在脚本里固化其文件名）。
EXCLUDE_SUFFIXES = ("-CHECKLIST.md",)
SCAN_EXTS = {".md", ".rs", ".sh", ".toml", ".yml", ".yaml", ".xml", ".java", ".proto", ".py"}
TOKEN_RE = re.compile(
    r"(?<![\w./-])(?:docs|scripts|jepsen|apis|deploy|monitoring)/[^\s`\"'()\[\]{}<>*…$|,;（）「」《》【】〔〕；，。！？：、]+"
)
TRAIL = ".,;:!?)]}，。；：！？）】》」』、"
URL_PREFIXES = ("http://", "https://", "mailto:", "ftp://")
# 运行时产物/生成目录（不在仓库内）：注释/文档中的正常写法，不做存在性校验。
RUNTIME_PREFIXES = ("jepsen/store/", "benchmark-results/")
# 本仓 doc-refs 负控制步骤（ci.yml）故意注入的缺失引用：跳过。
ALLOW_TOKENS = {"docs/__gate_self_check_missing__.md"}

fail = False


def is_placeholder(tok: str) -> bool:
    return any(ch in tok for ch in "<>{}[]*…$|")


def looks_like_ref(tok: str) -> bool:
    last = tok.rsplit("/", 1)[-1]
    return tok.endswith("/") or "." in last


def check_ref(path: pathlib.Path, line_no: int, tok: str) -> None:
    global fail
    tok = tok.split("#", 1)[0]
    tok = re.sub(r":\d+(-\d+)?$", "", tok)
    if not tok or is_placeholder(tok) or not looks_like_ref(tok):
        return
    if tok.startswith(RUNTIME_PREFIXES) or tok in ALLOW_TOKENS:
        return
    # 1) 相对文件所在目录（文档内相对写法） 2) 相对仓库根（注释内惯用写法）
    # 3) jepsen/ 下的文件常以 jepsen/ 为基准引用 scripts/...
    cands = [path.parent / tok, pathlib.Path(tok)]
    if str(path).startswith("jepsen/"):
        cands.append(pathlib.Path("jepsen") / tok)
    if not any(c.exists() for c in cands):
        print(f"DANGLING(ref): {path}:{line_no} -> {tok}")
        fail = True


for dirpath, dirnames, filenames in os.walk("."):
    dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
    for fn in filenames:
        if fn.endswith(EXCLUDE_SUFFIXES):
            continue
        ext = os.path.splitext(fn)[1]
        if ext not in SCAN_EXTS:
            continue
        p = pathlib.Path(dirpath) / fn
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if ext == ".md":
                for m in re.finditer(r"\]\(([^)]*)\)", line):
                    raw = m.group(1).split(" ", 1)[0].strip()
                    if raw.startswith("#") or raw.startswith(URL_PREFIXES):
                        continue
                    tgt = raw.split("#", 1)[0]
                    if not tgt or is_placeholder(tgt):
                        continue
                    cand = pathlib.Path(tgt) if tgt.startswith("/") else p.parent / tgt
                    if not cand.exists():
                        print(f"DANGLING(link): {p}:{i} -> {tgt}")
                        fail = True
            for m in TOKEN_RE.finditer(line):
                check_ref(p, i, m.group(0).rstrip(TRAIL))

if fail:
    print("check-doc-refs: FAIL（存在悬空引用）")
    sys.exit(1)
print("check-doc-refs: OK")
PY
rc=$?

# ---------- 警告（不置红）：Rust 注释中的日期 ----------
hits="$(grep -rEn '^[[:space:]]*//.*20[0-9]{2}-[0-9]{2}-[0-9]{2}' --include='*.rs' . 2>/dev/null \
  | grep -vE '/target/|coord-core/src/workflow/cron\.rs|coord-server/src/auth/token_signing\.rs|coord-agent/src/services/(idgen|event_notification)\.rs|coord-server/src/bff/internal\.rs' || true)"
if [ -n "$hits" ]; then
  echo "— 警告：Rust 注释含日期（白名单外）—"
  echo "$hits"
fi

exit "$rc"
