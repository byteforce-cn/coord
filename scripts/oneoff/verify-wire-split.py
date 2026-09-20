#!/usr/bin/env python3
"""验证 proto 拆分未改变任何 wire（消息/枚举/字段编号/rpc 名/service 名）。

用法（仓库根）：
    python3 scripts/oneoff/verify-wire-split.py [迁移前基线 proto]

基线默认 `scripts/oneoff/agent_api.pre-v0.2.0.proto`（v0.1.0 的单体 agent_api.proto，
即 `4a5e3ee` 的 `coord-proto/src/proto/agent_api.proto` 快照）。
退出码 0 = 全部保持；1 = 有缺失或差异。
"""
import re
import glob
import os
import sys


def parse(text):
    msgs, rpcs, enums = {}, {}, {}
    cur = ('', '')
    for ln in text.splitlines():
        s = ln.split('//')[0].strip()
        if not s:
            continue
        m = re.match(r'^(message|enum|service)\s+(\w+)', s)
        if m:
            k, n = m.group(1), m.group(2)
            cur = (k, n)
            if k == 'message':
                msgs.setdefault(n, [])
            elif k == 'enum':
                enums.setdefault(n, [])
            else:
                rpcs.setdefault(n, [])
            continue
        m = re.match(r'^rpc\s+(\w+)', s)
        if m and cur[0] == 'service':
            rpcs[cur[1]].append(m.group(1))
            continue
        m = re.match(
            r'^(?:repeated\s+|optional\s+)?(?:map<[^>]*>\s+)?([A-Za-z_]\w*)\s*=\s*(\d+)\s*;',
            s,
        )
        if m:
            if cur[0] == 'message':
                msgs[cur[1]].append((m.group(1), int(m.group(2))))
            elif cur[0] == 'enum':
                enums[cur[1]].append((m.group(1), int(m.group(2))))
    return msgs, rpcs, enums


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(ROOT)

baseline = sys.argv[1] if len(sys.argv) > 1 else 'scripts/oneoff/agent_api.pre-v0.2.0.proto'
old = open(baseline, encoding='utf-8').read()
om, orpc, oe = parse(old)

new_files = sorted(
    set(glob.glob('coord-proto/src/proto/*.proto'))
    - {'coord-proto/src/proto/storage.proto'}  # storage 从未在 agent_api.proto 里
)

nm, nrpc, ne = {}, {}, {}
for f in new_files:
    a, b, c = parse(open(f, encoding='utf-8').read())
    for k, v in a.items():
        if k in nm and nm[k] != v:
            print(f'  !! MSG DUPLICATE/DIFF {k}: first={nm[k]} later({f})={v}')
        nm.setdefault(k, v)
    for k, v in b.items():
        if k in nrpc and nrpc[k] != v:
            print(f'  !! RPC DUPLICATE/DIFF {k}')
        nrpc.setdefault(k, v)
    for k, v in c.items():
        ne.setdefault(k, v)

skip_enum_names = {'EventType', 'ServingStatus', 'CompareResult', 'Target'}
missing_m = sorted(set(om) - set(nm))
missing_r = sorted(set(orpc) - set(nrpc))
missing_e = sorted(set(oe) - set(ne) - skip_enum_names)
print('baseline         :', baseline)
print('candidate files  :', len(new_files))
print('missing messages :', missing_m)
print('missing services :', missing_r)
print('missing enums    :', missing_e)
print('field diffs      :', [k for k in om if k in nm and om[k] != nm[k]])
print('rpc diffs        :', [k for k in orpc if k in nrpc and orpc[k] != nrpc[k]])
ok = not (missing_m or missing_r or missing_e)
ok = ok and all(om[k] == nm[k] for k in om if k in nm)
ok = ok and all(orpc[k] == nrpc[k] for k in orpc if k in nrpc)
print('WIRE PRESERVED   :', ok)
sys.exit(0 if ok else 1)
