#!/usr/bin/env bash
# 把「会跑测试的 cargo target」切成 N 片，供 CI 并行构建 + 并行执行。
#
# 为什么现算而不是维护一份清单：手写清单会漂（新加的 tests/*.rs 没人记得加进去，而
# "漏跑"和"通过"在 CI 上是同一个绿色）。这里每次从 `cargo metadata` 枚举全部单元，
# 所以「某个单元没被分到任何一片」在构造上不存在；剩下唯一的失效面是**枚举本身**
# 与 cargo 的视野不一致，由 `verify` 子命令 fail-closed 兜住。
#
# 单元（unit）= 一个会跑测试的 cargo target：
#   <pkg>::lib            → -p <pkg> --lib
#   <pkg>::bin::<name>    → -p <pkg> --bin <name>
#   <pkg>::test::<name>   → -p <pkg> --test <name>
# 带 required-features 的 test target 不进分片（默认构建里不存在），但必须能在
# ci.yml 里找到真的会执行它的命令，否则 `verify` 变红。
#
# 用法：
#   ci-shard.sh plan                       # {"matrix":{"shard":[1..N]},"shards":N}（给 fromJSON）
#   ci-shard.sh selectors --shard I/N      # 该片的 cargo 选择符，**每行一个 token**（供 mapfile 直接用）
#   ci-shard.sh count --shard I/N          # 该片的单元数
#   ci-shard.sh units                      # 全部单元（人读，调试用）
#   ci-shard.sh verify --shards N          # 覆盖性门禁（fail-closed）：跑在汇总 job 里
#
# 退出码：0 成功；1 门禁失败；2 用法错误。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ci-shard.sh: 需要 python3" >&2
  exit 2
fi

exec python3 - "$REPO_ROOT" "$@" <<'PY'
import hashlib
import json
import os
import subprocess
import sys

REPO_ROOT = sys.argv[1]
ARGS = sys.argv[2:]

# 分片数只在这里定义：workflow 通过 `plan` 拿矩阵，汇总 job 通过 `plan` 拿 N。
# 「矩阵 3 条腿 / selectors I/N / verify N」写成三份就会漂，漂了就是某片永远不跑。
SHARDS = 3

# 分组盐。分组不承载语义（调它只是让某片多/少几个 target，没有正确性风险），
# 但直接决定三片的编译时间是否均匀。279 是按**实测每个 target 的编译耗时**（一次冷
# `--workspace --tests --timings`，见设计文档）搜索出来的：每片 harness 编译时间
# 19.7 / 20.3 / 20.1 分钟（偏差 2.9%），而按源码字节数挑的盐能偏到 40%+。
# 只有单元集明显变大时才需要重新搜索（搜法：拿 --timings 的 per-unit duration 当权重，
# 对 salt 求每片时间和的最大偏差）。
SALT = 279

MEMBER_ROOT_MARKER = "manifest_path"


def die(msg):
    print(f"ci-shard.sh: {msg}", file=sys.stderr)
    sys.exit(1)


def metadata():
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        die("需要 cargo（cargo metadata --no-deps）")
    except subprocess.CalledProcessError as exc:
        die(f"cargo metadata 失败：{exc.stderr.strip()[:400]}")
    return json.loads(out)


def _has(kinds, kind):
    return kind in kinds


def units(meta):
    """[(unit_id, selector_args, size_bytes)]，按 unit_id 排序（稳定输出）。"""
    out = []
    for pkg in meta["packages"]:
        pkg_name = pkg["name"]
        pkg_dir = os.path.dirname(pkg["manifest_path"])
        for target in pkg["targets"]:
            kinds = target["kind"]
            name = target["name"]
            if _has(kinds, "test"):
                if target.get("required-features"):
                    continue  # 默认构建里不存在；由 verify 检查它在 ci.yml 里真的被执行
                unit = f"{pkg_name}::test::{name}"
                size = _src_size(target)
                sel = ["-p", pkg_name, "--test", name]
            elif _has(kinds, "lib") or _has(kinds, "proc-macro"):
                unit = f"{pkg_name}::lib"
                size = _src_size(target)
                sel = ["-p", pkg_name, "--lib"]
            elif _has(kinds, "bin"):
                unit = f"{pkg_name}::bin::{name}"
                size = _src_size(target)
                sel = ["-p", pkg_name, "--bin", name]
            else:
                continue  # custom-build 等
            out.append((unit, sel, size, pkg_dir))
    out.sort(key=lambda row: row[0])
    return out


def _src_size(target):
    try:
        return os.path.getsize(target["src_path"])
    except OSError:
        return 0


def shard_of(unit_id, n):
    digest = hashlib.blake2b(f"{SALT}:{unit_id}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") % n + 1


def required_feature_targets(meta):
    out = []
    for pkg in meta["packages"]:
        for target in pkg["targets"]:
            if _has(target["kind"], "test") and target.get("required-features"):
                out.append(target["name"])
    return sorted(set(out))


def ci_yml_sim_commands():
    """从 ci.yml 里取出「同时含 cargo test 与 --features sim-tests」的 step 里 --test 的名字。"""
    path = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        die(f"读不到 {path}")
    found = set()
    # 按 step 切块：`      - name:` / `      - uses:` 开头即新 step
    step_starts = [
        i for i, line in enumerate(text.splitlines()) if line.startswith("      - ")
    ]
    lines = text.splitlines()
    bounds = step_starts + [len(lines)]
    for start, end in zip(bounds, bounds[1:]):
        block = "\n".join(lines[start:end])
        if "cargo test" not in block or "--features sim-tests" not in block:
            continue
        tokens = block.split()
        for i, tok in enumerate(tokens):
            if tok == "--test" and i + 1 < len(tokens):
                found.add(tokens[i + 1].strip("'\""))
    return found


def cmd_plan(_args):
    print(json.dumps({"matrix": {"shard": list(range(1, SHARDS + 1))}, "shards": SHARDS}))


def _parse_shard(args):
    if len(args) != 2 or args[0] != "--shard":
        die("用法：ci-shard.sh selectors|count --shard I/N")
    spec = args[1]
    if "/" not in spec:
        die(f"--shard 需要 I/N 形式，得到 {spec!r}")
    left, right = spec.split("/", 1)
    try:
        index, total = int(left), int(right)
    except ValueError:
        die(f"--shard 需要 I/N 形式，得到 {spec!r}")
    if not 1 <= index <= total:
        die(f"--shard {spec} 越界（I 必须在 1..N）")
    return index, total


def cmd_selectors(args):
    index, total = _parse_shard(args)
    all_units = units(metadata())
    picked = [u for u in all_units if shard_of(u[0], total) == index]
    if not picked:
        die(f"分片 {index}/{total} 没有任何单元——分组或分片数错了")
    # 每行一个 token：调用方用 `mapfile -t args < <(...)` 拿到就能直接
    # `cargo build "${args[@]}"`，不需要再做一层分词。
    for _unit, sel, _size, _dir in picked:
        for token in sel:
            print(token)


def cmd_count(args):
    index, total = _parse_shard(args)
    print(sum(1 for u in units(metadata()) if shard_of(u[0], total) == index))


def cmd_units(_args):
    all_units = units(metadata())
    for unit, _sel, size, _dir in all_units:
        print(f"{size:9d}  shard {shard_of(unit, SHARDS)}  {unit}")


def cmd_verify(args):
    total = SHARDS
    if args:
        if len(args) != 2 or args[0] != "--shards":
            die("用法：ci-shard.sh verify [--shards N]")
        total = int(args[1])
    if total != SHARDS:
        die(
            f"--shards {total} 与脚本内的分片数 {SHARDS} 不一致："
            "矩阵与门禁必须来自同一处（用 `plan` 的输出）"
        )

    meta = metadata()
    all_units = units(meta)
    if not all_units:
        die("枚举到 0 个单元")

    # ① 每个单元恰好落在一个分片；② 每片非空；索引不得越界。
    per_shard = {}
    for unit, _sel, size, _dir in all_units:
        bucket = shard_of(unit, total)
        if not 1 <= bucket <= total:
            die(f"单元 {unit} 被分到越界分片 {bucket}（应在 1..{total}）")
        per_shard.setdefault(bucket, []).append((unit, size))
    missing = [i for i in range(1, total + 1) if i not in per_shard]
    if missing:
        die(f"分片 {missing} 为空——有单元没被分到任何一片")

    # ③ workspace 成员目录下一层的 tests/*.rs 必须都能对上某个 test target。
    #    盯的是「枚举本身和 cargo 的视野不一致」：autotests = false、[[test]] 只在
    #    子目录里、或新 crate 用了自定义 test 路径。只看成员目录（非成员的 spike crate
    #    不在 cargo 视野里，不该让门禁变红）。
    targets_by_pkg = {}
    for pkg in meta["packages"]:
        pkg_dir = os.path.dirname(os.path.realpath(pkg["manifest_path"]))
        names = set()
        for target in pkg["targets"]:
            if _has(target["kind"], "test"):
                names.add(os.path.realpath(target["src_path"]))
        targets_by_pkg[pkg_dir] = names
    orphans = []
    for pkg_dir, target_paths in targets_by_pkg.items():
        tests_dir = os.path.join(pkg_dir, "tests")
        if not os.path.isdir(tests_dir):
            continue
        for entry in sorted(os.listdir(tests_dir)):
            if not entry.endswith(".rs"):
                continue
            path = os.path.realpath(os.path.join(tests_dir, entry))
            if not os.path.isfile(path):
                continue
            if path not in target_paths:
                orphans.append(os.path.relpath(path, REPO_ROOT))
    if orphans:
        die(
            "这些测试文件不在任何 cargo test target 里（枚举会漏跑它们）：\n  "
            + "\n  ".join(orphans)
        )

    # ④ required-features 的 target 必须有一条 ci.yml 命令真的执行它们。
    sim_targets = required_feature_targets(meta)
    if sim_targets:
        wired = ci_yml_sim_commands()
        unwired = [name for name in sim_targets if name not in wired]
        if unwired:
            die(
                "这些 target 需要 --features 才存在，但 ci.yml 里没有一条 "
                "`cargo test … --features sim-tests … --test <name>` 会执行它们："
                + ", ".join(unwired)
            )

    # 报告：单元数 + 源码字节数（后者是编译成本的主要代理）。
    print(f"OK: {len(all_units)} 个单元覆盖到 {total} 个分片；"
          f"required-features 目标 {len(sim_targets)} 个已在 ci.yml 接线")
    for index in range(1, total + 1):
        rows = per_shard[index]
        print(f"  shard {index}/{total}: {len(rows):3d} 个单元, "
              f"{sum(size for _u, size in rows) / 1024:.0f} KiB 源码")


COMMANDS = {
    "plan": cmd_plan,
    "selectors": cmd_selectors,
    "count": cmd_count,
    "units": cmd_units,
    "verify": cmd_verify,
}

if not ARGS or ARGS[0] not in COMMANDS:
    die(f"用法：ci-shard.sh {{{'|'.join(COMMANDS)}}} [args]")
COMMANDS[ARGS[0]](ARGS[1:])
PY
