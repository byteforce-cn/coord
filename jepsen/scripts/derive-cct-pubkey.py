#!/usr/bin/env python3
"""由 server 根密钥（`[security].auth_root_key`）派生 agent 需要的 Ed25519 CCT 公钥。

## 为什么需要这个脚本

server 用 Ed25519 签发 CCT（`TokenSigningKeyring::ed25519_signing_key`），而
**agent 只做验签**（这样任一 agent 被控也无法伪造 root token —— 这正是对称
HMAC 方案的根因修复）。所以部署 agent 时必须把对应的**公钥**配进去
（`[auth].verifying_key_hex`）。

派生路径与 server 完全一致（`coord-server/src/auth/token_signing.rs:196-208`）：

    seed = HKDF-SHA256(ikm = auth_root_key, salt = None(→ 32 个 0 字节),
                       info = "coord-cct-ed25519-v1", L = 32)
    verifying_key = Ed25519(seed).public_key()

## 重要发现（交给 coord 团队的缺项）

当前**没有任何自动化路径**能拿到这个公钥：CLI 没有子命令、Auth 服务没有
`GetCctPublicKey` 之类的 RPC、启动日志也不打印它。也就是说运维必须自己派生
（本脚本）或从代码里算 —— 这是 M5a 落地过程中实际卡住的一步，已记入
`jepsen/docs/coord-agent-coverage-plan.md` §7 的待确认清单（§3 的 AG-05 相邻项：
「部署期密钥同步」）。

## 用法

    python3 derive-cct-pubkey.py <auth_root_key_hex>
    python3 derive-cct-pubkey.py <auth_root_key_hex> --check <expected_hex>

`--check` 不一致时退出码 1（供 lab 的门禁用：根密钥换了而常量没跟着换，
必须**在起跑前**发现，而不是等到 agent 启动后一堆 UNAUTHENTICATED）。

依赖：`cryptography`（只需在本机派生一次；lab 侧不依赖它 —— 派生结果作为
常量存在 `db.clj`，本脚本用于重新派生与一致性检查）。
"""
import argparse
import hashlib
import hmac
import sys

HKDF_INFO = b"coord-cct-ed25519-v1"


def hkdf_sha256(ikm: bytes, info: bytes, length: int, salt: bytes = b"\x00" * 32) -> bytes:
    """RFC 5869 HKDF-SHA256（`Hkdf::new(None, ikm)` 的 salt 就是全 0）。"""
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    okm = b""
    t = b""
    counter = 1
    while len(okm) < length:
        t = hmac.new(prk, t + info + bytes([counter]), hashlib.sha256).digest()
        okm += t
        counter += 1
    return okm[:length]


def derive(root_key_hex: str) -> str:
    root = bytes.fromhex(root_key_hex.strip())
    seed = hkdf_sha256(root, HKDF_INFO, 32)
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import (
            Encoding, PublicFormat)
    except ImportError:
        sys.exit("cryptography is required to derive the Ed25519 public key "
                 "(pip install cryptography); the derived value is committed as a "
                 "lab constant, this script is only needed to re-derive it")
    sk = Ed25519PrivateKey.from_private_bytes(seed)
    return sk.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root_key_hex", help="[security].auth_root_key (hex)")
    ap.add_argument("--check", metavar="EXPECTED_HEX",
                    help="fail (exit 1) unless the derived key equals this value")
    args = ap.parse_args()

    derived = derive(args.root_key_hex)
    if args.check:
        if derived == args.check.strip().lower():
            print(f"OK  {derived}")
            return 0
        print(f"MISMATCH\n  derived : {derived}\n  expected: {args.check.strip().lower()}",
              file=sys.stderr)
        return 1
    print(derived)
    return 0


if __name__ == "__main__":
    sys.exit(main())
