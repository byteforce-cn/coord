#!/usr/bin/env bash
#
# T0.4 —— 环境清理与 preflight（幂等，可在任意时刻重复执行）。
#
# 在**被测节点**上以 root 运行（由控制机经 SSH 调用，或由 nemesis 的
# `:stop` 路径调用）。清掉上一次 run 的残留——残留的 iptables DROP 规则、
# netem qdisc、孤儿 coord 进程、时钟偏移、磁盘填充文件——并**验证**清理结果；
# 验证不通过时返回非 0，让调用方拒绝起跑（§0-5 环境幂等）。
#
# 用法：
#   env-reset.sh [选项]
#
#   --wipe-data DIR    删除 DIR 下的所有内容（run 边界用；默认不删数据目录，
#                      因为 nemesis 的 :stop 路径**绝不能**清数据——那会毁掉
#                      kill -9 的崩溃持久化语义）
#   --keep-partitions  只删 netem/jepsen 相关规则，不整表 flush iptables
#                      （默认 flush，lab 节点是独占的）
#   --network-only     只重置网络/磁盘填充（iptables、tc、disk-fill），**不**
#                      杀进程、不校时钟、不碰数据目录。nemesis 的 :stop 路径
#                      必须用这个模式：跑在 :stop 里的 pkill coord 会杀掉
#                      nemesis 刚刚重启/恢复的那个进程，把 kill/pause 的
#                      “停→恢复”语义整个破坏掉。
#   --clock-max-ms N   时钟偏移上界，默认 100
#   --clock-ref HOST   以 HOST 的墙上时钟为基准校验本机偏移（控制机执行时传
#                      各节点名；不传则用本机 chrony/ntp 的自报偏移）
#   --clock-offset-ms N
#                      调用方**已测得**的本机偏移（毫秒）。env-reset-all.sh 用
#                      这条路径：在控制机上用 NTP 式夹逼采样（t0 = 发送前、
#                      t1 = 节点当前时间、t2 = 收到后，offset = t1 - (t0+t2)/2）
#                      得到偏移和往返时延。它不需要被测节点装 chrony/ntpq
#                      （docker 镜像里没有），也不需要节点能 ssh 到别的机器。
#   --clock-uncertainty-ms N
#                      与 --clock-offset-ms 配套的测量不确定度（≈ RTT/2）。
#                      判定规则（R2：必须声明漏检边界）：
#                        off - U > 上界             -> FAIL（可证超过上界）
#                        off + U > 上界（但上面不成立） -> WARN（精度不足，无法判定）
#                        否则                       -> OK
#                      高 RTT 的链路上本来就无法审计 100ms 上界，把这种情况
#                      报成 FAIL 只能产出假红（F-10 的 docker lab 就是）。
#   --clock-source S   与 --clock-offset-ms 配套，报告里标注测量方法（不要带
#                      括号/空格：它会被拼进远端 bash -c 的命令行）
#   --strict-clock     没有**任何**时钟校验路径可用时报错而不是 WARN
#   --quiet            只输出失败项与最终结论
#
# 退出码：0 = 环境干净（或仅 WARN）；1 = 有硬校验失败；2 = 用法错误。

set -uo pipefail

WIPE_DATA=""
KEEP_PARTITIONS=0
NETWORK_ONLY=0
CLOCK_MAX_MS=100
CLOCK_REF=""
CLOCK_OFFSET_MS=""
CLOCK_UNCERTAINTY_MS=0
CLOCK_SOURCE="caller-measured"
STRICT_CLOCK=0
QUIET=0

COORD_PATTERN='/opt/[c]oord/coord'

while [[ $# -gt 0 ]]; do
    case "$1" in
        --wipe-data)      WIPE_DATA="${2:-}"; shift 2 ;;
        --keep-partitions) KEEP_PARTITIONS=1; shift ;;
        --network-only)   NETWORK_ONLY=1; shift ;;
        --clock-max-ms)   CLOCK_MAX_MS="${2:-100}"; shift 2 ;;
        --clock-ref)      CLOCK_REF="${2:-}"; shift 2 ;;
        --clock-offset-ms) CLOCK_OFFSET_MS="${2:-}"; shift 2 ;;
        --clock-uncertainty-ms) CLOCK_UNCERTAINTY_MS="${2:-0}"; shift 2 ;;
        --clock-source)   CLOCK_SOURCE="${2:-caller-measured}"; shift 2 ;;
        --strict-clock)   STRICT_CLOCK=1; shift ;;
        --quiet)          QUIET=1; shift ;;
        -h|--help)        sed -n '3,28p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

FAILURES=0
WARNINGS=0

say()  { [[ $QUIET -eq 1 ]] || echo "$@"; }
ok()   { say "  OK   $*"; }
warn() { echo "  WARN $*" >&2; WARNINGS=$((WARNINGS + 1)); }
fail() { echo "  FAIL $*" >&2; FAILURES=$((FAILURES + 1)); }
# ---------------------------------------------------------------------------
# 1. iptables —— jepsen 的分区 nemesis 用 DROP 规则实现；残留规则会让下一次
#    run 在"网络本来就断的集群"上起跑，产出无法归因的红。
# ---------------------------------------------------------------------------
reset_iptables() {
    command -v iptables >/dev/null 2>&1 || { warn "iptables not installed; skipped"; return; }
    if [[ $KEEP_PARTITIONS -eq 1 ]]; then
        # 只删 jepsen 的分区规则（-j DROP/REJECT 的目标匹配）
        iptables -S 2>/dev/null | grep -E -- '-j (DROP|REJECT)' | while read -r rule; do
            # shellcheck disable=SC2086
            iptables ${rule/-A /-D } 2>/dev/null || true
        done
    else
        iptables -F 2>/dev/null || true
        iptables -t nat -F 2>/dev/null || true
        iptables -t mangle -F 2>/dev/null || true
        iptables -Z 2>/dev/null || true
    fi

    local left
    left=$(iptables -S 2>/dev/null | grep -cE -- '-j (DROP|REJECT)' || true)
    if [[ "${left:-0}" -gt 0 ]]; then
        fail "iptables still has ${left} DROP/REJECT rule(s)"
    else
        ok "iptables clean (no DROP/REJECT rules)"
    fi
}

# ---------------------------------------------------------------------------
# 2. tc/netem —— 时延/丢包 nemesis 的 qdisc 残留会让"没有注入故障"的窗口
#    依然有丢包，可用率门槛（T0.2）会误判。
# ---------------------------------------------------------------------------
reset_tc() {
    command -v tc >/dev/null 2>&1 || { warn "tc not installed; skipped"; return; }
    local ifaces
    ifaces=$(ip -o link show 2>/dev/null | awk -F': ' '$2 != "lo" {print $2}' | cut -d@ -f1)
    local left=0
    for dev in $ifaces; do
        tc qdisc del dev "$dev" root 2>/dev/null || true
        tc qdisc del dev "$dev" ingress 2>/dev/null || true
        if tc qdisc show dev "$dev" 2>/dev/null | grep -qE 'netem|tbf|htb'; then
            left=$((left + 1))
            echo "       leftover qdisc on $dev: $(tc qdisc show dev "$dev" 2>/dev/null | head -3)" >&2
        fi
    done
    if [[ $left -gt 0 ]]; then
        fail "tc qdisc residue on ${left} interface(s)"
    else
        ok "tc/netem clean"
    fi
}

# ---------------------------------------------------------------------------
# 3. 孤儿 coord 进程 —— 上一轮跑完没退干净的进程会占端口/持有数据目录，
#    让下一轮以"半个集群"起跑（仓内 chaos 门禁就被这个坑过一次）。
# ---------------------------------------------------------------------------
reset_processes() {
    pkill -9 -f "$COORD_PATTERN" 2>/dev/null || true
    sleep 1
    local left
    left=$(pgrep -f "$COORD_PATTERN" 2>/dev/null | wc -l)
    if [[ "${left:-0}" -gt 0 ]]; then
        fail "${left} coord process(es) still alive after SIGKILL"
    else
        ok "no stray coord processes"
    fi
}

# ---------------------------------------------------------------------------
# 4. 磁盘填充文件 —— T3.5 的 disk-full nemesis 用 fallocate 造满磁盘。
# ---------------------------------------------------------------------------
reset_disk_fill() {
    local n=0
    for f in /tmp/coord-diskfill* /var/tmp/coord-diskfill*; do
        [[ -e "$f" ]] || continue
        rm -f "$f" && n=$((n + 1))
    done
    ok "disk-fill files removed (${n})"
}

# ---------------------------------------------------------------------------
# 5. 时钟 —— 时钟偏移会污染 Lease/CCT 判定（§5.4-⑤）。先强制同步，再**验证**
#    偏移 < --clock-max-ms。
# ---------------------------------------------------------------------------
reset_clock() {
    local offset_ms="" src=""

    if command -v chronyc >/dev/null 2>&1; then
        chronyc makestep >/dev/null 2>&1 || true
        local secs
        secs=$(chronyc tracking 2>/dev/null \
               | awk -F': *' '/^System time/ {print $2}' \
               | awk '{print $1}')
        if [[ -n "$secs" ]]; then
            offset_ms=$(awk -v s="$secs" 'BEGIN {printf "%.1f", (s < 0 ? -s : s) * 1000}')
            src="chrony"
        fi
    fi

    if [[ -z "$offset_ms" ]] && command -v ntpq >/dev/null 2>&1; then
        local ms
        ms=$(ntpq -c rv 2>/dev/null | grep -o 'offset=[-0-9.]*' | head -1 | cut -d= -f2)
        if [[ -n "$ms" ]]; then
            offset_ms=$(awk -v m="$ms" 'BEGIN {printf "%.1f", (m < 0 ? -m : m)}')
            src="ntpq"
        fi
    fi

    if [[ -z "$offset_ms" && -n "$CLOCK_REF" ]]; then
        command -v ssh >/dev/null 2>&1 || { warn "ssh missing; cannot check --clock-ref"; return; }
        local here there
        here=$(date +%s%3N)
        there=$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
                    "$CLOCK_REF" date +%s%3N 2>/dev/null) || { warn "clock-ref $CLOCK_REF unreachable"; return; }
        # 忽略 ssh 往返（<~50ms）带来的粗差；这里只需要抓分钟级偏移
        offset_ms=$((here > there ? here - there : there - here))
        src="clock-ref:$CLOCK_REF"
    fi

    # --clock-offset-ms：调用方（控制机）用 NTP 式夹逼采样测得的偏移。
    # 这里不再另做换算：ssh 握手与 stdin 传脚本的开销无法在本进程内测量，
    # 所以基准必须由**能同时看到两端时钟**的一方提供。
    if [[ -z "$offset_ms" && -n "$CLOCK_OFFSET_MS" ]]; then
        offset_ms=$CLOCK_OFFSET_MS
        src="$CLOCK_SOURCE"
    fi

    if [[ -z "$offset_ms" ]]; then
        if [[ $STRICT_CLOCK -eq 1 ]]; then
            fail "no clock sync tool (chrony/ntpq) and no --clock-ref/--clock-offset-ms: cannot verify offset"
        else
            warn "no clock sync tool available; offset unverified (use --strict-clock to enforce)"
        fi
        return
    fi

    # 判定：只在**可证**超过上界时 FAIL；精度不足（测量不确定度吃掉整个上界）
    # 时只 WARN —— 高 RTT 链路上无法审计 100ms 时钟，把它当 FAIL 只会产出假红。
    local lower
    lower=$(awk -v o="$offset_ms" -v u="$CLOCK_UNCERTAINTY_MS" \
            'BEGIN {v = o - u; if (v < 0) v = 0; printf "%.1f", v}')
    if awk -v l="$lower" -v m="$CLOCK_MAX_MS" 'BEGIN {exit !(l > m)}'; then
        fail "clock offset ${offset_ms}ms (+/-${CLOCK_UNCERTAINTY_MS}ms, ${src}) > ${CLOCK_MAX_MS}ms"
    elif awk -v o="$offset_ms" -v u="$CLOCK_UNCERTAINTY_MS" -v m="$CLOCK_MAX_MS" \
              'BEGIN {exit !(o + u > m)}'; then
        warn "clock offset ${offset_ms}ms +/-${CLOCK_UNCERTAINTY_MS}ms (${src}): 测量精度不足以判定 <= ${CLOCK_MAX_MS}ms"
    else
        ok "clock offset ${offset_ms}ms +/-${CLOCK_UNCERTAINTY_MS}ms <= ${CLOCK_MAX_MS}ms (${src})"
    fi
}

# ---------------------------------------------------------------------------
# 6. 数据目录（仅 run 边界；nemesis :stop 路径不传 --wipe-data）
# ---------------------------------------------------------------------------
reset_data() {
    [[ -z "$WIPE_DATA" ]] && return
    if [[ "$WIPE_DATA" == "/" || "$WIPE_DATA" == "/var" || "$WIPE_DATA" == "/opt" ]]; then
        fail "refusing to wipe $WIPE_DATA"
        return
    fi
    rm -rf "${WIPE_DATA:?}"/* 2>/dev/null || true
    ok "data dir wiped: $WIPE_DATA"
}

# ---------------------------------------------------------------------------

say "== env-reset $(hostname) $(date -u +%Y-%m-%dT%H:%M:%SZ) $([[ $NETWORK_ONLY -eq 1 ]] && echo '(network-only)') =="
reset_iptables
reset_tc
if [[ $NETWORK_ONLY -eq 0 ]]; then
    reset_processes
    reset_clock
    reset_data
fi
reset_disk_fill

if [[ $FAILURES -gt 0 ]]; then
    echo "env-reset: ${FAILURES} check(s) FAILED, ${WARNINGS} warning(s) — environment NOT clean" >&2
    exit 1
fi
say "env-reset: clean (${WARNINGS} warning(s))"
exit 0
