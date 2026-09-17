#!/usr/bin/env bash
# soak-diag.sh — one-shot diagnostics for a running soak, run ON the control
# node (uploaded via vagrant upload). Avoids all the nested ssh quoting.
set -u

LOG=/root/coord-test/soak/soak.log
STORE=/root/coord-test/store/coord

echo "==== host: $(hostname)  date: $(date '+%F %T') ===="
echo "-- soak.log: $(wc -l < "$LOG") lines, $(du -h "$LOG" | cut -f1) --"
echo "-- log first line: $(head -n 1 "$LOG") --"
echo "-- log last line:  $(tail -n 1 "$LOG") --"

echo
echo "-- op type counts over whole soak log --"
for t in :ok :info :fail :no-client :unauthenticated; do
  echo "$t: $(grep -c "$t" "$LOG")"
done

echo
echo "-- first :no-client line --"
grep -m1 ':no-client' "$LOG"
echo "-- last :ok line --"
grep ':ok' "$LOG" | tail -n 1

echo
echo "-- recent nemesis history (last 25) --"
grep ':nemesis' "$LOG" | tail -n 25

echo
echo "-- nemesis events between 17:40 and 18:15 --"
grep ':nemesis' "$LOG" | grep -E '17:4[0-9]|17:5[0-9]|18:0[0-9]|18:1[0-5]'

echo
echo "-- per-node state --"
NODES=(n1 n2 n3 n4 n5)
for h in "${NODES[@]}"; do
  # Remote script runs via `bash -s` from stdin: no ssh-side quoting at all.
  # 2>&1 merges ssh errors into the section, and the section ALWAYS prints.
  OUT=$(
    ssh -o ConnectTimeout=8 -o StrictHostKeyChecking=no \
      -i /root/.ssh/id_ed25519 root@"$h" 2>&1 bash -s <<'REMOTE'
set +e
H=$(hostname)
echo "[$H] coord process:"
ps -eo pid,etime,rss,pcpu,cmd | grep -E '[c]oord' | grep -v grep | head -n 3
if ! pgrep -x coord >/dev/null 2>&1; then echo "  (no coord process)"; fi
echo "[$H] http :50061 -> $(curl -sk -m 3 -o /dev/null -w '%{http_code}' http://127.0.0.1:50061/ 2>/dev/null || echo no-response)"
echo "[$H] grpc :50051 -> $(timeout 3 bash -c '</dev/tcp/127.0.0.1/50051' 2>/dev/null && echo open || echo closed)"
echo "[$H] /var/log/coord.log: $(stat -c 'mtime %y size %s' /var/log/coord.log 2>/dev/null || echo MISSING)"
echo "[$H] coord.log tail:"
tail -n 15 /var/log/coord.log 2>/dev/null || echo "  (no coord.log)"
echo "[$H] dmesg oom:"
dmesg 2>/dev/null | grep -iE 'killed process|out of memory' | tail -n 5 || echo "  (none / dmesg unavailable)"
echo "[$H] data dir:"
ls -la /var/lib/coord 2>/dev/null | head -n 8 || echo "  (none)"
echo "[$H] mem:"
free -m | head -n 2
REMOTE
  )
  echo "==== $h ===="
  if [ -z "$OUT" ]; then
    echo "  SSH RETURNED NO OUTPUT (unreachable?)"
  else
    echo "$OUT"
  fi
done

echo
echo "-- latest store dir --"
ls -1dt "$STORE"/*/ 2>/dev/null | head -n 1
