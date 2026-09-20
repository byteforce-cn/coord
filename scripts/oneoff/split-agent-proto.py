#!/usr/bin/env python3
"""一次性迁移脚本（v0.2.0，**已执行完毕，仅为留档**）。

用法（仓库根）：`python3 scripts/oneoff/split-agent-proto.py`
前置：`coord-proto/src/proto/agent_api.proto` 仍是迁移前的单体文件
     （快照见同目录 `agent_api.pre-v0.2.0.proto`）。

把 `coord-proto/src/proto/agent_api.proto` 按 service 块拆分为 per-domain proto 文件，
package 由 `coord.agent` 改为契约包名。

设计约束（见 docs/coord-agent-ga-v0.2.0-plan-2026-09-19.md §4.2）：
  - 迁移 = 包名与文件位置的搬移；字段编号 / rpc 名 / service 名逐字不动。
  - 契约副本 (apis/contracts/proto/...) 与实现副本 (coord-proto/src/proto/...)
    内容逐字相同 => check-wire-sync.sh 两层校验天然通过。
"""
import os
import re

SRC = "coord-proto/src/proto/agent_api.proto"
CONTRACT_DIR = "apis/contracts/proto/coord"
IMPL_DIR = "coord-proto/src/proto"

# service -> (domain, 中文标题, GA 期限, 备注)
META = {
    "Registry": ("registry", "服务注册与发现", "2026-10-31", ""),
    "Lock": ("lock", "分布式锁", "2026-11-30", ""),
    "LeaderElection": ("election", "Leader 选举", "2026-11-30", ""),
    "IdGen": ("idgen", "分布式 ID", "2026-10-31", ""),
    "Event": ("event", "事件通知", "2026-12-31", ""),
    "Config": ("config", "配置中心", "2026-12-31", ""),
    "Pki": ("pki", "PKI 证书签发", "2026-12-31", ""),
    "Policy": (
        "policy",
        "权限策略引擎（OPA）",
        "2026-12-31",
        "边界声明：OPA bundle 存于 coord-server KV（跨 Agent 共享）；\n"
        "// RBAC 策略为 **Agent 本地内存**，不承诺跨 Agent 共享（WHITEPAPER §10 规则 4）。",
    ),
    "CircuitBreaker": (
        "circuitbreaker",
        "熔断器",
        "2026-12-31",
        "边界声明：熔断器状态为 **Agent 本地内存**，不承诺跨 Agent 共享\n"
        "// （WHITEPAPER §10 规则 4）；重启即重置。",
    ),
    "RateLimiter": (
        "ratelimiter",
        "限流器",
        "2026-12-31",
        "边界声明：令牌桶为 **Agent 本地内存**，不承诺跨 Agent 共享\n"
        "// （WHITEPAPER §10 规则 4）；重启即重置。",
    ),
    "Transit": ("transit", "安全传输（信封加密）", "2026-12-31", ""),
    "Cache": ("cache", "缓存", "2026-12-31", ""),
    "MQ": (
        "mq",
        "消息队列",
        "2026-12-31",
        "说明：wire 服务名保留实现原样 `MQ`（迁移只改 package，不改 service 名；\n"
        "// 计划书 §4.2 表中写的 `Mq` 系笔误，实际实现为 `MQ`）。",
    ),
    "Workflow": ("workflow", "工作流（Saga）", "2027-03-31", ""),
    "Scheduler": ("scheduler", "分布式调度", "2027-03-31", ""),
    "FeatureFlags": ("featureflags", "特性开关", "2026-12-31", ""),
}

KEEP = {"Handshake", "Health", "Replica"}

HEADER = '''syntax = "proto3";

// ============================================================
// Coord 对外契约 — {title}（能力承诺面，v1.2）
//
// 状态：COMMITTED（承诺中）｜ GA 期限：{deadline} ｜ 台账：apis/contracts/STATUS.md
// wire 基线：coord-proto/src/proto/{domain}.proto 的 coord.{domain}.v1.{svc}
//            （字段编号/类型逐项一致；迁移 = 机械式重挂载，禁止趁机改 wire）
//{note}
// GA 验收定义与倒逼机制见 WHITEPAPER.md §13。
// ============================================================

package coord.{domain}.v1;

option java_multiple_files = true;
option java_package = "cn.byteforce.coord.contracts.{domain}.v1";
option go_package = "github.com/byteforce/coord/apis/contracts/gen/go/coord/{domain}/v1;{domain}v1";
'''


def main() -> int:
    raw = open(SRC, encoding="utf-8").read()
    lines = raw.split("\n")
    n = len(lines)

    banners = [i for i, l in enumerate(lines) if re.match(r"^// ={10,}$", l)]
    secs = []
    for i, b in enumerate(banners):
        end = banners[i + 1] if i + 1 < len(banners) else n
        svc = None
        for j in range(b, end):
            m = re.match(r"^service (\w+)", lines[j])
            if m:
                svc = m.group(1)
                break
        if svc:
            secs.append([svc, b, None])
    for i in range(len(secs)):
        secs[i][2] = secs[i + 1][1] if i + 1 < len(secs) else n

    print("sections:", [(s[0], s[1] + 1, s[2]) for s in secs])

    def body(a, b):
        return "\n".join(lines[a:b]).strip("\n")

    written = []
    for svc, a, b in secs:
        if svc in KEEP:
            continue
        domain, title, deadline, note = META[svc]
        note_line = ("//\n// " + note + "\n") if note else ""
        content = HEADER.format(
            title=title, deadline=deadline, note=note_line, domain=domain, svc=svc
        ) + "\n" + body(a, b) + "\n"

        cdir = os.path.join(CONTRACT_DIR, domain, "v1")
        os.makedirs(cdir, exist_ok=True)
        cpath = os.path.join(cdir, f"{domain}.proto")
        if os.path.exists(cpath):
            print(f"  contract exists, impl follows contract: {cpath}")
            content = open(cpath, encoding="utf-8").read()
        else:
            open(cpath, "w", encoding="utf-8").write(content)
            print(f"  + contract {cpath}")

        ipath = os.path.join(IMPL_DIR, f"{domain}.proto")
        open(ipath, "w", encoding="utf-8").write(content)
        written.append(ipath)
        print(f"  + impl     {ipath}")

    # 重建 agent_api.proto：仅保留内部面 Handshake / Health / Replica
    keep_secs = [s for s in secs if s[0] in KEEP]
    out = [
        'syntax = "proto3";',
        "package coord.agent;",
        "",
        "option java_multiple_files = true;",
        'option java_package = "cn.byteforce.coord.sdk.internal.proto";',
        "",
        "// ============================================================",
        "// coord.agent —— 内部面（不对业务消费者承诺）",
        "//",
        "// 本文件只保留三类内部服务：",
        "//   - Handshake：协议版本协商（WHITEPAPER 防空头承诺；两端均须实现）",
        "//   - Health：探活（agent 内部健康检查）",
        "//   - Replica：ISR 复制通道（WHITEPAPER §9.3 红线 R3「永不对外」）",
        "//",
        "// 其余 16 个服务已于 contracts/v1.2.0 迁出为独立契约包",
        "// （package coord.<domain>.v1），本文件不再包含它们。",
        "// ============================================================",
        "",
    ]
    for svc, a, b in keep_secs:
        out.append(body(a, b))
        out.append("")
    open(SRC, "w", encoding="utf-8").write("\n".join(out).rstrip("\n") + "\n")
    print(f"  ~ rewritten {SRC} (kept {[s[0] for s in keep_secs]})")
    print(f"total impl files: {len(written)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
