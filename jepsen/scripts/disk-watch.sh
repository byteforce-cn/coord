#!/usr/bin/env bash
# disk-watch.sh — host-side disk & leak watchdog for the coord jepsen lab.
#
# Why (2026-09-27 soak incident):
# during a 72h soak the shared 936G overlay filled up (watermark WARN
# 13.2% free at 03:31Z -> READ-ONLY ~0% at 05:15Z), cascading into hard kills
# of coord/agent processes (05:44Z) and, after a power outage, a full lab
# restart. ~700G of space was released by process death / reboot, pointing at
# deleted-but-open files. This watcher samples free space and per-process
# deleted-open bytes so a recurrence is attributed to a concrete container +
# pid:comm within one interval.
#
# Run it on the docker host (this workspace), with control+nodes up:
#   bash jepsen/scripts/disk-watch.sh --once
#   OUTDIR=jepsen/store/disk-watch \
#     setsid nohup bash jepsen/scripts/disk-watch.sh loop 120 \
#       >> jepsen/store/disk-watch/disk-watch.log 2>&1 < /dev/null &
#
# Output (one row per host + per container per sample):
#   ts,scope,host_avail_kb,host_pct,live_kb,log_b,wm,nproc,del_kb,del_top,del_top_kb,note
#   - host_pct = df use%; alerts fire when FREE% < 30 (i.e. use% > 70)
#   - wm       = <WARN count>/<READ-ONLY count> of disk-watermark lines in /var/log/coord.log
#   - del_kb   = bytes held by deleted-but-open files, summed over processes;
#                del_top = "pid:comm" of the largest holder
# Also logs ALERTs to $OUTDIR/disk-watch.log: del_kb > 5G on any container,
# any new READ-ONLY watermark, or host free% < 30.
set -u

OUTDIR=${OUTDIR:-jepsen/store/disk-watch}
MODE=${1:-loop}
INTERVAL=${2:-120}
CONTAINERS=${CONTAINERS:-"control n1 n2 n3 n4 n5"}
PREFIX=${PREFIX:-jepsen-}
CSV=$OUTDIR/disk-watch.csv
LOG=$OUTDIR/disk-watch.log
RO_STATE=$OUTDIR/state_last_ro

mkdir -p "$OUTDIR"
if [ ! -f "$CSV" ]; then
  echo "ts,scope,host_avail_kb,host_pct,live_kb,log_b,wm,nproc,del_kb,del_top,del_top_kb,note" > "$CSV"
fi

# Inner script: executed inside each container via `docker exec -i <c> bash -s`.
# Pure bash + coreutils; read-only.
INNER=$(mktemp)
trap 'rm -f "$INNER"' EXIT
cat > "$INNER" <<'INNER_EOF'
set -u
live=$(du -sk /var/lib/coord 2>/dev/null | awk '{print $1}'); live=${live:-0}
logb=$(stat -c%s /var/log/coord.log 2>/dev/null); logb=${logb:-0}
warn=$(grep -c 'disk watermark WARN' /var/log/coord.log 2>/dev/null); warn=${warn:-0}
ro=$(grep -c 'disk watermark READ-ONLY' /var/log/coord.log 2>/dev/null); ro=${ro:-0}
nproc=$(ps -eo comm= 2>/dev/null | grep -c '^coord'); nproc=${nproc:-0}
tot=0; tpid="-"; tb=0
for d in /proc/[0-9]*; do
  [ -d "$d/fd" ] || continue
  bytes=0
  for l in "$d"/fd/*; do
    tgt=$(readlink "$l") || continue
    case "$tgt" in
      *" (deleted)") ;;
      *) continue ;;
    esac
    sz=$(stat -Lc %s "$l" 2>/dev/null) || sz=0
    bytes=$((bytes + sz))
  done
  if [ "$bytes" -gt 0 ]; then
    tot=$((tot + bytes))
    if [ "$bytes" -gt "$tb" ]; then
      tb=$bytes; tpid="${d#/proc/}:$(cat "$d/comm" 2>/dev/null || echo '?')"
    fi
  fi
done
printf 'LIVE_KB=%s LOG_B=%s WM=%s/%s NPROC=%s DEL_KB=%s DEL_TOP=%s DEL_TOP_KB=%s\n' \
  "$live" "$logb" "$warn" "$ro" "$nproc" "$((tot / 1024))" "$tpid" "$((tb / 1024))"
INNER_EOF

sample() {
  local ts avail pct total_ro c out note
  ts=$(date -u +%FT%TZ)
  read -r avail pct < <(df -Pk / | awk 'NR==2 {print $4, $5}')
  echo "$ts,host,$avail,$pct,0,0,0/0,0,0,-,0,ok" >> "$CSV"
  total_ro=0
  for c in $CONTAINERS; do
    out=$(docker exec -i "${PREFIX}${c}" bash -s < "$INNER" 2>/dev/null) || out=""
    local live logb wm nproc del tpid tkb
    live=$(printf '%s' "$out" | sed -n 's/.*LIVE_KB=\([0-9]*\).*/\1/p'); live=${live:-0}
    logb=$(printf '%s' "$out" | sed -n 's/.*LOG_B=\([0-9]*\).*/\1/p'); logb=${logb:-0}
    wm=$(printf '%s' "$out" | sed -n 's/.*WM=\([0-9]*\/[0-9]*\).*/\1/p'); wm=${wm:-0/0}
    nproc=$(printf '%s' "$out" | sed -n 's/.*NPROC=\([0-9]*\).*/\1/p'); nproc=${nproc:-0}
    del=$(printf '%s' "$out" | sed -n 's/.*DEL_KB=\([0-9]*\).*/\1/p'); del=${del:-0}
    tpid=$(printf '%s' "$out" | sed -n 's/.*DEL_TOP=\([^ ]*\).*/\1/p'); tpid=${tpid:--}
    tkb=$(printf '%s' "$out" | sed -n 's/.*DEL_TOP_KB=\([0-9]*\).*/\1/p'); tkb=${tkb:-0}
    total_ro=$((total_ro + ${wm#*/}))
    note=ok
    if [ "$del" -gt 5242880 ]; then note="WARN deleted-open>5GB"; fi
    echo "$ts,$c,$avail,$pct,$live,$logb,$wm,$nproc,$del,$tpid,$tkb,$note" >> "$CSV"
    if [ "$note" != ok ]; then
      echo "[$ts] WARN $c deleted-open=${del}KB top=${tpid} (${tkb}KB) host_free=${pct}%" >> "$LOG"
    fi
  done
  local ap free
  ap=${pct%\%}
  free=$((100 - ${ap:-0}))
  if [ "$free" -lt 30 ]; then
    echo "[$ts] ALERT host free=${free}% (avail=${avail}KB)" >> "$LOG"
  fi
  local last_ro=-1
  [ -f "$RO_STATE" ] && last_ro=$(cat "$RO_STATE" 2>/dev/null || echo -1)
  if [ "$total_ro" -gt "$last_ro" ] && [ "$last_ro" -ge 0 ] && [ "$total_ro" -gt 0 ]; then
    echo "[$ts] ALERT disk watermark READ-ONLY count rose: ${last_ro} -> ${total_ro}" >> "$LOG"
  fi
  echo "$total_ro" > "$RO_STATE"
  echo "[$ts] sample ok (host free ${free}%)" >> "$LOG"
}

if [ "$MODE" = "--once" ]; then
  sample
  tail -n 8 "$CSV"
  exit 0
fi

echo "[$(date -u +%FT%TZ)] disk-watch start interval=${INTERVAL}s containers='${CONTAINERS}'" >> "$LOG"
while true; do
  sample
  sleep "$INTERVAL"
done
