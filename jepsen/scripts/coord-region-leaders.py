#!/usr/bin/env python3
"""coord-region-leaders.py — per-region leader/election metrics from coord logs.

Usage (control node):  python3 coord-region-leaders.py [STORE_DIR]

STORE_DIR defaults to the latest /root/coord-test/store/coord/*/ . Parses each
node's snarfed coord.log (n1/coord.log, ...) for raft_wm read-served lines:

    ... raft_wm region=1 node=1 state=Leader ... term=1 ... read_served

For each region reports:
  - leader changes  : how many times the node serving linearizable reads for
                      that region changed (proxy for elections/leadership
                      moves, e.g. after the leader is killed/paused)
  - leader timeline : (time-offset-s, node) of each change
  - leader share    : fraction of read-served lines per node (leadership time
                      proxy), i.e. per-region leader distribution

Single-node soak nemesis kills/pauses ONE node; each region has a replica on
that node, so regions led by it must re-elect independently — regions led by
other nodes must be unaffected (no change). That is the T4.4 isolation check.
"""
import re
import sys
import glob
import os

def store_dir():
    if len(sys.argv) > 1:
        return sys.argv[1]
    base = "/root/coord-test/store/coord"
    latest = os.path.join(base, "latest")
    if os.path.islink(latest):
        return os.path.realpath(latest)
    dirs = sorted(glob.glob(os.path.join(base, "*")))
    return dirs[-1] if dirs else base

def parse_logs(store):
    events = {}
    # coord logs carry ANSI colors (--log-format pretty): strip them so the
    # timestamp can anchor the line.
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    pat = re.compile(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*?raft_wm "
        r"region=(\d+) node=(\d+) state=\w+ leader=Some\(\d+\) term=\d+"
        r".*read_served")
    for i in (1, 2, 3):
        path = os.path.join(store, f"n{i}", "coord.log")
        if not os.path.exists(path):
            print(f"  (no {path})")
            continue
        with open(path, errors="replace") as f:
            for raw in f:
                line = ansi.sub("", raw)
                m = pat.search(line)
                if m:
                    try:
                        import datetime
                        ep = datetime.datetime.strptime(
                            m.group(1), "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
                    except Exception:
                        continue
                    rid = int(m.group(2))
                    node = int(m.group(3))
                    events.setdefault(rid, []).append((ep, node))
    return events

def main():
    store = store_dir()
    print(f"store: {store}")
    events = parse_logs(store)
    if not events:
        print("no raft_wm read-served lines found (no coord.logs in store?)")
        sys.exit(0)
    t0 = min(e[0] for es in events.values() for e in es)
    print(f"first raft_wm (epoch s): {t0:.0f}")
    for rid in sorted(events):
        es = sorted(events[rid])
        # leader changes: consecutive distinct serving node
        changes = []
        cur = None
        for ep, node in es:
            if node != cur:
                if cur is not None:
                    changes.append((ep - t0, ep, cur, node))
                cur = node
        dist = {}
        for _, node in es:
            dist[node] = dist.get(node, 0) + 1
        total = len(es)
        share = {k: round(v / total, 3) for k, v in sorted(dist.items())}
        print(f"\nregion {rid}: read-served lines={total} "
              f"leader changes={len(changes)} leader share={share}")
        for off, ep, old, new in changes[:15]:
            print(f"    t+{off:7.1f}s (epoch {ep:.0f})  leader {old} -> {new}")
        if len(changes) > 15:
            print(f"    ... ({len(changes) - 15} more changes)")

if __name__ == "__main__":
    main()
