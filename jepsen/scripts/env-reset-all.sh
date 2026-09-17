#!/usr/bin/env bash
#
# T0.4 —— 在**所有节点**（控制机 + /root/nodes 里的每台）上跑 env-reset.sh。
#
# 在控制机上运行。这是 run 边界的 preflight：pkill 残留、清 iptables/tc、
# 校时钟偏移、必要时清数据目录。**每个 run 起跑前必须全绿**（§0-5 环境幂等）：
# 任何一台节点没通过就直接拒绝起跑，避免"在没清干净的环境上跑出的红"无法归因。
#
# 用法（控制机）：
#   scripts/env-reset-all.sh [--wipe-data /var/lib/coord] [--strict-clock] [...]
#
#   参数原样透传给每个节点上的 env-reset.sh（见该脚本的 --help）。
#   --wipe-data 只应在 run 边界用；nemesis 的 :stop 路径绝不能用。
#
# 退出码：0 = 全部节点干净；1 = 有节点失败（逐台打印失败原因）。

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/env-reset.sh"
NODES_FILE="${NODES_FILE:-/root/nodes}"
KEY="${SSH_PRIVATE_KEY:-/root/.ssh/id_ed25519}"

if [[ ! -f "$SCRIPT" ]]; then
    echo "env-reset-all: missing $SCRIPT" >&2
    exit 2
fi

failures=0
declare -a failed_nodes=()

# F-10：被测节点可能没有 chrony/ntpq（jepsen-docker 的 node 镜像就没有），
# 而 `--strict-clock` 曾因此**恒红**，让「每个 run 起跑前 preflight 必须全绿」
# （§0-5）在 docker lab 上无法满足。修法不是放宽检查，而是换一个可测的基准：
# 真正要保证的是**节点之间**的钟差（Lease/CCT 判定都以此为输入），而控制机
# 就是天然的基准。控制机用 NTP 式夹逼采样量出偏移后再交给节点脚本判定：
#
#   t0 = 控制机发送前； t1 = 节点当前时间； t2 = 控制机收到后
#   offset = t1 - (t0 + t2) / 2          # 正 = 节点快
#   rtt    = t2 - t0                     # 测量不确定度（≈ 2×单向时延）
#
# 这样 ssh 握手与「传脚本」的开销都不在测量窗口里（它们发生在 t0..t2 之外
# 或被夹逼抵消），残差只剩单向时延的一半。
clock_bracket() {
    local host="$1" key="$2" t0 t1 t2 rtt
    t0=$(date +%s%3N)
    t1=$(ssh -i "$key" -o BatchMode=yes -o StrictHostKeyChecking=no \
             -o ConnectTimeout=10 "root@$host" date +%s%3N 2>/dev/null) || return 1
    t2=$(date +%s%3N)
    rtt=$((t2 - t0))
    local off=$(( t1 - (t0 + t2) / 2 ))
    # 传给节点脚本的是**绝对偏移**；正负都带，节点脚本按上界比较。
    echo "$off $rtt"
}

run_here() {
    echo "----- $(hostname) (control) -----"
    # 控制机与自身比较：偏移恒为 0（报告里标注基准来源）。
    if ! bash "$SCRIPT" "$@" --clock-offset-ms 0 --clock-source self; then
        failures=$((failures + 1)); failed_nodes+=("$(hostname)(control)")
    fi
}

run_remote() {
    local node="$1"; shift
    echo "----- $node -----"
    local meas off rtt u extra
    if meas=$(clock_bracket "$node" "$KEY"); then
        off="${meas%% *}"; rtt="${meas##* }"
        u=$(( rtt / 2 ))
        # 标签里不能出现括号/空格：它会被拼进远端 bash -c 的命令行。
        extra="--clock-offset-ms ${off#-} --clock-uncertainty-ms $u --clock-source control-bracket"
    else
        echo "  WARN clock-bracket sampling failed on $node" >&2
        extra=""
    fi
    if ! ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=no \
             -o ConnectTimeout=10 "root@$node" \
             "bash -s -- $* $extra" < "$SCRIPT"; then
        failures=$((failures + 1)); failed_nodes+=("$node")
    fi
}

echo "== env-reset-all $(date -u +%Y-%m-%dT%H:%M:%SZ) args: $* =="
run_here "$@"

if [[ -f "$NODES_FILE" ]]; then
    while read -r node; do
        [[ -z "$node" ]] && continue
        run_remote "$node" "$@"
    done < "$NODES_FILE"
else
    echo "env-reset-all: no nodes file at $NODES_FILE (control only)" >&2
fi

echo
if [[ $failures -gt 0 ]]; then
    echo "env-reset-all: FAILED on: ${failed_nodes[*]}" >&2
    echo "  → 环境不干净，拒绝在该环境上起跑（§0-5）" >&2
    exit 1
fi
echo "env-reset-all: all nodes clean"
exit 0
