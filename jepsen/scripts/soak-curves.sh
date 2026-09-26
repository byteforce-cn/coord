#!/usr/bin/env bash
#
# soak-curves.sh — 长跑曲线采集（控制机侧、detached）。
#
# 目的（P5 / W3-7）：长跑（72h / ≥14 天）期间按固定间隔采样四条曲线的原始数据：
#   内存   rss_sum_kb            —— 该节点上 coord / coord-agent 进程 RSS 之和
#   磁盘   disk_kb               —— /var/lib/coord 数据目录大小（du -sk）
#   重启   restarts_total        —— 活进程 pid 变化或 etimes 回退 ⇒ +1
#                                （进程缺席窗口不更新基线 ⇒ 一次 kill 只计 1 次）
#   挂起   keepalive_failed_total / snapshot_not_found_total
#                                —— 节点日志（/var/log/coord.log）的**增量**计数
#
# 输出（<OUTDIR>/）：
#   curves.csv   长表：每节点每间隔一行（11 列，见文件头注释行）
#   meta.txt     开始时间 / 间隔 / 节点 / 脚本 sha256 / 关联的 store run 目录
#   state/       每节点增量与重启检测状态（持久化 —— 采样器自身重启不丢；
#                重启检测跨采样器重启继续有效）
#   sampler.log  运行日志
#
# 用法（控制机，在 /root/coord-test 下）：
#   ./scripts/soak-curves.sh start <OUTDIR> [间隔秒=300] [节点...]
#   ./scripts/soak-curves.sh once  <OUTDIR> [节点...]   # 采一轮（自检；同时初始化 state）
#   ./scripts/soak-curves.sh status
#   ./scripts/soak-curves.sh report <OUTDIR>
#   ./scripts/soak-curves.sh stop
#
# 约定：
#   - 与 run 目录的关系：14 天 run 建议 <OUTDIR> = store/coord/latest/curves
#     （collect-evidence.sh 会把 curves/ 一并归档并计入 sha256sums）。
#   - 时间戳用控制机时钟（单一时钟轴；与 lab 内跨节点时钟偏移无关）。
#   - 节点默认取 /root/nodes；单节点不可达 ⇒ 该行计 SSH_FAIL，不推进其 state
#     （下轮重新对齐），**不**中断整轮采样。
#   - 采样器不随 soak 自动停止：run 结束后手动 stop。
set -uo pipefail

PROJECT="${PROJECT:-/root/coord-test}"
SOAK_DIR="$PROJECT/soak"
PIDFILE="$SOAK_DIR/soak-curves.pid"
KEY=/root/.ssh/id_ed25519

usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

warn() { echo "[soak-curves $(date -u +%FT%TZ)] $*"; }

running_pid() {
  [[ -s "$PIDFILE" ]] || return 1
  local pid; pid=$(head -1 "$PIDFILE")
  kill -0 "$pid" 2>/dev/null && { echo "$pid"; return 0; }
  return 1
}

default_nodes() {
  if [[ -s /root/nodes ]]; then awk 'NF {print $1}' /root/nodes
  else printf 'n1\nn2\nn3\n'; fi
}

# 采一个节点。$1=node $2=上一字节偏移（state）。stdout 一行：
#   OK,pid,etimes_s,nprocs,rss_sum_kb,disk_kb,log_bytes,cka_delta,csn_delta,rotated
#   FAIL                      —— ssh 不可达
# 远端只读；不写任何东西。
sample_node() {
  local node=$1 off=$2 raw
  raw=$(ssh -o ConnectTimeout=8 -o StrictHostKeyChecking=no -o BatchMode=yes \
           -i "$KEY" "root@$node" bash -s -- "$off" 2>/dev/null <<'REMOTE'
set +e
off="$1"
PS=$(ps -eo pid=,etimes=,rss=,comm= 2>/dev/null \
     | awk '$4 ~ /^coord/ {n++; rss+=$3; if (p=="") {p=$1; e=$2}} \
            END {printf "%s %s %s %s", (p==""?0:p), (p==""?0:e), n+0, rss+0}')
set -- $PS
DISK=$(du -sk /var/lib/coord 2>/dev/null | awk '{print $1}'); DISK=${DISK:-0}
L=/var/log/coord.log
SZ=$(stat -c%s "$L" 2>/dev/null); SZ=${SZ:-0}
if [ "$SZ" -ge "$off" ]; then
  ROT=0
  CKA=$(tail -c +$((off+1)) "$L" 2>/dev/null | grep -c 'keep-alive failed')
  CSN=$(tail -c +$((off+1)) "$L" 2>/dev/null | grep -c 'snapshot not found')
else
  ROT=1
  CKA=$(grep -c 'keep-alive failed' "$L" 2>/dev/null)
  CSN=$(grep -c 'snapshot not found' "$L" 2>/dev/null)
fi
CKA=${CKA:-0}; CSN=${CSN:-0}
echo "OK,$1,$2,$3,$4,$DISK,$SZ,$CKA,$CSN,$ROT"
REMOTE
  ) || true
  if [[ "$raw" == OK,* ]]; then
    printf '%s\n' "$raw"
  else
    printf 'FAIL\n'
  fi
}

# 一轮采样：逐节点采样并追加一行到 curves.csv。
# state 文件：<last_live_pid> <last_live_etimes> <restarts> <ka_total> <sn_total> <off>
# 重启口径：只跟踪**活进程**的 pid/etimes —— 进程缺席（pid=0，nemesis kill 窗口）
# 不更新 last_live_*，因此「123→缺席→Y」只计 1 次（回到 Y 时 123→Y 不等）。
sample_cycle() {
  local outdir=$1; shift
  local nodes=("$@")
  local csv="$outdir/curves.csv"
  local ts; ts=$(date -u +%FT%TZ)
  local node raw st
  for node in "${nodes[@]}"; do
    st="$outdir/state/$node"
    local prev_pid=- prev_et=0 restarts=0 ka=0 sn=0 off=0
    if [[ -f "$st" ]]; then
      read -r prev_pid prev_et restarts ka sn off < "$st" 2>/dev/null || true
      prev_pid=${prev_pid:--}; prev_et=${prev_et:-0}
      restarts=${restarts:-0}; ka=${ka:-0}; sn=${sn:-0}; off=${off:-0}
    fi
    raw=$(sample_node "$node" "$off")
    if [[ "$raw" == FAIL ]]; then
      printf '%s,%s,,,,,,,%s,%s,%s\n' "$ts" "$node" "$restarts" "$ka" "$sn" >> "$csv"
      warn "SSH_FAIL $node（state 未推进；下轮重试）"
      continue
    fi
    local ok pid et n rss disk lb cka csn rot
    IFS=, read -r ok pid et n rss disk lb cka csn rot <<< "$raw"
    if [[ "$pid" != "0" ]]; then
      if [[ "$prev_pid" != "-" ]]; then
        if [[ "$pid" != "$prev_pid" ]]; then
          restarts=$((restarts + 1))
          warn "RESTART $node: pid $prev_pid -> $pid (et $prev_et -> $et; 累计 $restarts)"
        elif (( et < prev_et - 10 )); then
          restarts=$((restarts + 1))
          warn "RESTART $node: pid $pid 不变但 etimes 回退（$prev_et -> $et; 累计 $restarts）"
        fi
      fi
      prev_pid=$pid; prev_et=$et
    fi
    ka=$((ka + cka)); sn=$((sn + csn)); off=$lb
    (( rot == 1 )) && warn "LOG_ROTATED $node: 日志回退，已全量重扫（off=$lb）"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$ts" "$node" "$pid" "$et" "$n" "$rss" "$disk" "$lb" "$restarts" "$ka" "$sn" >> "$csv"
    printf '%s %s %s %s %s %s\n' "$prev_pid" "$prev_et" "$restarts" "$ka" "$sn" "$off" > "$st"
  done
}

csv_header() {
  echo "# ts_utc,node,pid,etimes_s,nprocs,rss_sum_kb,disk_kb,log_bytes,restarts_total,keepalive_failed_total,snapshot_not_found_total"
}

cmd_once() {
  local outdir=${1:-}; shift || true
  [[ -n "$outdir" ]] || usage
  local nodes
  if [[ $# -gt 0 ]]; then nodes=("$@"); else mapfile -t nodes < <(default_nodes); fi
  mkdir -p "$outdir/state"
  [[ -f "$outdir/curves.csv" ]] || csv_header > "$outdir/curves.csv"
  sample_cycle "$outdir" "${nodes[@]}"
  tail -n "$((${#nodes[@]} + 1))" "$outdir/curves.csv"
}

cmd_start() {
  local outdir=${1:-}; shift || true
  [[ -n "$outdir" ]] || usage
  local interval=${1:-300}; shift || true
  local nodes
  if [[ $# -gt 0 ]]; then nodes=("$@"); else mapfile -t nodes < <(default_nodes); fi

  local pid; pid=$(running_pid || true)
  if [[ -n "$pid" ]]; then
    echo "soak-curves already running (pid $pid); stop it first" >&2
    exit 1
  fi

  mkdir -p "$outdir/state"
  [[ -f "$outdir/curves.csv" ]] || csv_header > "$outdir/curves.csv"
  local rundir; rundir=$(ls -1dt "$PROJECT"/store/coord/*/ 2>/dev/null | head -1)
  cat > "$outdir/meta.txt" <<EOF
script   = soak-curves.sh
sha256   = $(sha256sum "$0" | awk '{print $1}')
started  = $(date -u +%FT%TZ)
interval = ${interval}s
nodes    = ${nodes[*]}
store    = ${rundir:-unknown}
EOF

  # 先采一轮（初始化 state；节点可能正在 nemesis 窗口 —— 失败不阻塞）
  sample_cycle "$outdir" "${nodes[@]}"

  setsid bash "$0" _loop "$outdir" "$interval" "${nodes[@]}" \
      >> "$outdir/sampler.log" 2>&1 < /dev/null &
  local lpid=$!
  # pidfile 两行：pid + outdir（第二行供 status 用；pidfile 本身在 .gitignore 的 *.pid/.log 规则内）
  printf '%s\n%s\n' "$lpid" "$outdir" > "$PIDFILE"
  echo "$lpid" > "$outdir/sampler.pid"
  sleep 2
  if kill -0 "$lpid" 2>/dev/null; then
    echo "curves sampler started (pid $lpid) -> $outdir (interval ${interval}s, nodes: ${nodes[*]})"
  else
    echo "sampler exited immediately — check $outdir/sampler.log" >&2
    rm -f "$PIDFILE"
    exit 1
  fi
}

cmd_loop() {
  local outdir=$1 interval=$2; shift 2
  trap 'warn "loop stopped"; exit 0' TERM INT
  warn "loop start: outdir=$outdir interval=$interval nodes=$*"
  while true; do
    sample_cycle "$outdir" "$@"
    sleep "$interval"
  done
}

cmd_status() {
  local pid; pid=$(running_pid || true)
  if [[ -n "$pid" ]]; then
    echo "== soak-curves RUNNING (pid $pid) =="
  else
    echo "== soak-curves NOT running =="
  fi
  local outdir=""
  if [[ -s "$PIDFILE" ]]; then outdir=$(sed -n '2p' "$PIDFILE"); fi
  if [[ -n "$outdir" && -f "$outdir/curves.csv" ]]; then
    echo "-- $outdir/curves.csv: $(( $(wc -l < "$outdir/curves.csv") - 1 )) 行 --"
    tail -n 4 "$outdir/curves.csv"
    [[ -f "$outdir/sampler.log" ]] && { echo "-- sampler.log --"; tail -n 5 "$outdir/sampler.log"; }
  fi
  [[ -z "$pid" ]] && exit 1 || exit 0
}

cmd_report() {
  local outdir=${1:-}; [[ -n "$outdir" ]] || usage
  local csv="$outdir/curves.csv"
  [[ -f "$csv" ]] || { echo "no curves.csv in $outdir" >&2; exit 1; }
  awk -F, '
    $1 ~ /^#/ { next }
    {
      node=$2; n[node]++
      if ($6 != "" && ($6+0) > rssmax[node]) rssmax[node]=$6+0
      if ($6 != "" && first[node]=="") first[node]=$6+0
      if ($6 != "") last[node]=$6+0
      if ($9 != "") rest[node]=$9+0
      if ($10 != "") ka[node]=$10+0
      if ($11 != "") sn[node]=$11+0
      if ($6 == "") fail[node]++
      ts[node]=$1
    }
    END {
      printf "%-6s %6s %11s %11s %11s %10s %7s %7s %7s\n", "node","rows","rss_first_kb","rss_max_kb","rss_last_kb","restarts","ka_fail","snap_miss","ssh_fail"
      for (x in n)
        printf "%-6s %6d %11d %11d %11d %10d %7d %7d %7d\n", x, n[x], first[x], rssmax[x], last[x], rest[x], ka[x], sn[x], fail[x]
    }' "$csv"
}

cmd_stop() {
  local pid; pid=$(running_pid || true)
  if [[ -z "$pid" ]]; then
    echo "soak-curves not running"
    rm -f "$PIDFILE" "$SOAK_DIR/curves.outdir"
    exit 0
  fi
  echo "stopping soak-curves (session $pid)…"
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  sleep 2
  kill -KILL -- "-$pid" 2>/dev/null || true
  rm -f "$PIDFILE"
  echo "stopped."
}

# start 之后把 outdir 记下来，status 用
case "${1:-}" in
  start)  shift; cmd_start "$@" ;;
  once)   shift; cmd_once "$@" ;;
  _loop)  shift; cmd_loop "$@" ;;
  status) cmd_status ;;
  report) shift; cmd_report "$@" ;;
  stop)   cmd_stop ;;
  *) usage ;;
esac
