#!/usr/bin/env bash
# ============================================================
# check-sdk-sync.sh — 对外契约 ↔ Java SDK 生成类型的**包命名空间**卡口（G3）
#
# 为什么需要它
# ------------
# contracts/v1.2.0 之前，SDK 的生成源是内部副本 `coord-proto/src/proto/*.proto`，
# 而 `package` 是 `coord.<domain>`（无 `.v1`）⇒ SDK 生成到
# `cn.byteforce.coord.sdk.internal.proto`，**与契约包
# `cn.byteforce.coord.contracts.<domain>.v1` 不是同一套类型**。
# 迁移后（契约与实现包名逐字相同）这条自动消解，但**很容易漂回去**：
# 只要有人给某个 impl 文件重新加上一行 `import ...sdk.internal.proto.*`，
# 编译仍然会过 —— 因为它恰好还残留着旧的生成类 —— 而契约面与实现面从此分叉。
# 本卡口把"漂回去"变成机器可见的红。
#
# 判据（三层）
# ------------
#   1) GA 服务的 SDK impl 文件**必须**引用契约命名空间
#      `cn.byteforce.coord.contracts.<domain>.v1`；
#   2) 同一批文件**不得**引用 `cn.byteforce.coord.sdk.internal.proto`
#      （保留的**内部面**除外，见下方 allowlist：目前只有 Health，
#        由 CoordClient 的 healthCheck() 使用）；
#   3) 反向：`sdk.internal.proto` 的使用面**只能**是 allowlist 里的文件 ——
#      否则说明有新的文件漂到内部面而没被登记（登记本身就是"这是刻意的"的证据）。
#
# 与其它卡口的分工
# ----------------
#   * `check-wire-sync.sh`      —— 契约 ↔ 内部副本的路径级/字段级一致 + 承诺期限
#   * `check-wire-descriptor.sh` —— 结构性 wire 签名（protoc FileDescriptorSet 反解）
#   * 本脚本                    —— 契约 ↔ **Java 生成命名空间**（防 SDK 面漂移）
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SDK_SRC="$REPO_ROOT/coord-java-sdk/src/main/java"
CONTRACT_PKG_PREFIX="cn.byteforce.coord.contracts."
INTERNAL_PKG="cn.byteforce.coord.sdk.internal.proto"

fail=0

if [[ ! -d "$SDK_SRC" ]]; then
  echo "FAIL: SDK 源码目录不存在：$SDK_SRC" >&2
  exit 1
fi

# ── allowlist：**刻意**仍使用内部面的文件。每一条都必须写明理由 ────────────
#    无理由的"暂时留着"就是漂移，所以这里只有两类：
#      ① 保留面（Handshake / Health）—— 契约里就没有对外包（红线 R3 / §4.2 表尾）；
#      ② 握手/协商通道 —— 它必须在"知道对方讲哪个版本"之前可用，因而不能依赖
#         任何 GA 面（P0-4 / D6）。
ALLOW_INTERNAL=(
  # healthCheck() 用保留面 Health（V3 的显式例外）
  "cn/byteforce/coord/sdk/CoordClient.java"
  # 协议版本协商：/coord.agent.Handshake/Negotiate 是**刻意保留的内部面**
  # （见计划书 §4.2 表尾 + 红线 R3），不建对外契约包
  "cn/byteforce/coord/sdk/internal/channel/AgentChannelManager.java"
)

is_allowed() {
  local rel="$1"
  for a in "${ALLOW_INTERNAL[@]}"; do
    [[ "$rel" == "$a" ]] && return 0
  done
  return 1
}

# ── GA 服务 → 其 impl 文件必须引用的契约包 ────────────────────────────────
#    <impl 文件相对路径>|<必须出现的契约包>
GA_IMPLS=(
  "cn/byteforce/coord/sdk/internal/rpc/RegistryImpl.java|contracts.registry.v1"
  "cn/byteforce/coord/sdk/internal/rpc/LockClientImpl.java|contracts.lock.v1"
  "cn/byteforce/coord/sdk/internal/rpc/IdGenClientImpl.java|contracts.idgen.v1"
  "cn/byteforce/coord/sdk/internal/rpc/ConfigClientImpl.java|contracts.config.v1"
  "cn/byteforce/coord/sdk/internal/rpc/PkiClientImpl.java|contracts.pki.v1"
  "cn/byteforce/coord/sdk/internal/rpc/PolicyClientImpl.java|contracts.policy.v1"
  "cn/byteforce/coord/sdk/internal/rpc/TransitClientImpl.java|contracts.transit.v1"
  "cn/byteforce/coord/sdk/internal/rpc/CacheClientImpl.java|contracts.cache.v1"
  "cn/byteforce/coord/sdk/internal/rpc/MqClientImpl.java|contracts.mq.v1"
  "cn/byteforce/coord/sdk/internal/rpc/WorkflowClientImpl.java|contracts.workflow.v1"
  "cn/byteforce/coord/sdk/internal/rpc/ObjectStoreClientImpl.java|contracts.storage"
  "cn/byteforce/coord/sdk/internal/rpc/LeaderElectionClientImpl.java|contracts.election.v1"
  "cn/byteforce/coord/sdk/internal/rpc/EventClientImpl.java|contracts.event.v1"
  "cn/byteforce/coord/sdk/internal/rpc/SchedulerClientImpl.java|contracts.scheduler.v1"
  "cn/byteforce/coord/sdk/internal/rpc/CircuitBreakerClientImpl.java|contracts.circuitbreaker.v1"
  "cn/byteforce/coord/sdk/internal/rpc/RateLimiterClientImpl.java|contracts.ratelimiter.v1"
  "cn/byteforce/coord/sdk/internal/rpc/FeatureFlagClientImpl.java|contracts.featureflags.v1"
)

echo "── 1) GA 服务的 impl 必须引用契约命名空间 ──"
for entry in "${GA_IMPLS[@]}"; do
  rel="${entry%%|*}"
  want="${entry##*|}"
  f="$SDK_SRC/$rel"
  if [[ ! -f "$f" ]]; then
    echo "FAIL: 清单里的 impl 不存在：$rel（清单本身过期了？）" >&2
    fail=1
    continue
  fi
  if ! grep -q "$want" "$f"; then
    echo "FAIL: $rel 未引用契约包 '$want' —— SDK 面可能已与契约分叉" >&2
    fail=1
  fi
  # 2) 同一文件不得引用内部 proto（allowlist 除外）
  if grep -q "$INTERNAL_PKG" "$f"; then
    if ! is_allowed "$rel"; then
      echo "FAIL: $rel 引用了内部 proto '$INTERNAL_PKG'（该文件不在 allowlist 里）" >&2
      fail=1
    fi
  fi
done

echo "── 2) 'sdk.internal.proto' 的使用面只能是 allowlist ──"
mapfile -t users < <(grep -rl "$INTERNAL_PKG" "$SDK_SRC" 2>/dev/null | sed "s|^$SDK_SRC/||" | sort)
if (( ${#users[@]} == 0 )); then
  echo "WARN: 全仓已无 '$INTERNAL_PKG' 引用 —— allowlist 可考虑清空" >&2
fi
for rel in "${users[@]}"; do
  if ! is_allowed "$rel"; then
    echo "FAIL: $rel 使用了内部面 '$INTERNAL_PKG' 但未被登记为刻意保留" >&2
    echo "      （若确为保留面，请连同理由加入本脚本的 ALLOW_INTERNAL）" >&2
    fail=1
  else
    echo "OK（allowlist）: $rel"
  fi
done

echo "── 3) 契约命名空间必须至少被 GA impl 用满清单 ──"
used=0
for entry in "${GA_IMPLS[@]}"; do
  rel="${entry%%|*}"
  [[ -f "$SDK_SRC/$rel" ]] && grep -q "$CONTRACT_PKG_PREFIX" "$SDK_SRC/$rel" && used=$((used + 1))
done
echo "OK: ${used}/${#GA_IMPLS[@]} 个 GA impl 使用契约命名空间"

# ── 4) 反向覆盖：GA 契约包**逐个**必须有对应的 SDK impl（G4）────────────────
#    为什么需要它：上面那张 GA_IMPLS 是**人写的**。P1-3（"Java SDK 缺 6 个客户端面"）
#    正是它漂移的结果 —— 契约已 COMMITTED，SDK 面却少了一个，而当时没有任何机器
#    守卫会发现。这与 `is_streaming_rpc` 的教训同源：**清单是人记得改的，卡口不是。**
#    判据来源：契约目录本身（不是另一张人写的清单）。
#      层 1 `coord/<domain>/v1/*.proto` → 包 `coord.<domain>.v1`（GA 层，必须有个 impl）
#      层 2 无 `.v1` 后缀（STABLE 底座原语 + `coord.storage`）→ 仅 `coord.storage`
#            要求 impl（底座原语由 SDK 内部原语面消费，不是独立客户端能力）
echo "── 4) 反向覆盖：每个 GA 契约包都必须有 SDK impl ──"
CONTRACT_PROTO_DIR="$REPO_ROOT/apis/contracts/proto"
expected_pkgs=()
while IFS= read -r f; do
  pkg="$(sed -n 's/^[[:space:]]*package[[:space:]]\+\([^;]*\);.*/\1/p' "$f" | head -1)"
  rel="${f#"$CONTRACT_PROTO_DIR"/}"
  case "$rel" in
    grpc/health/*) continue ;;                       # 第三方标准 proto，随附
    coord/kv/*|coord/txn/*|coord/lease/*|coord/watch/*|coord/maintenance/*) continue ;;
  esac
  [[ -z "$pkg" ]] && continue
  expected_pkgs+=("$pkg")
done < <(find "$CONTRACT_PROTO_DIR" -name '*.proto' | sort)

for pkg in "${expected_pkgs[@]}"; do
  # 归一化：契约 proto 的包名是 `coord.<domain>...`，GA_IMPLS 里写的是
  # Java 命名空间后缀 `contracts.<domain>...`。两边都剥掉前缀后逐字比较，
  # 这样清单与契约目录的**唯一**约定就是 domain 段本身。
  norm="${pkg#coord.}"
  found=0
  for entry in "${GA_IMPLS[@]}"; do
    want="${entry##*|}"
    [[ "${want#contracts.}" == "$norm" ]] && found=1 && break
  done
  if (( found == 0 )); then
    echo "FAIL: 契约包 '$pkg' 已进契约面，但 GA_IMPLS 里没有对应的 SDK impl" >&2
    echo "      ⇒ 该能力对 Java 消费者**没有客户端面**（P1-3 同型）。" >&2
    echo "      修法：补 SDK 客户端并加到本脚本的 GA_IMPLS，或在契约里降级该能力。" >&2
    fail=1
  else
    echo "OK: $pkg"
  fi
done
if (( fail == 0 )); then
  echo "OK: ${#expected_pkgs[@]} 个 GA 契约包全部有 SDK impl"
fi

if (( fail != 0 )); then
  echo "SDK ↔ 契约命名空间不一致：见上方 FAIL" >&2
  exit 1
fi
echo "sdk-sync OK：GA 服务的 SDK impl 全部落在契约命名空间，内部面使用面已被登记，且契约包无 SDK 覆盖缺口。"
