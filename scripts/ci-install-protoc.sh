#!/usr/bin/env bash
# 安装 protoc：`coord-proto` 的 `prost-build` 依赖，也是 descriptor 级契约校验的输入。
#
# apt 优先（Ubuntu 24.04 自带 3.21.12）；GitHub hosted runner 上 Ubuntu 镜像偶发不可达
# （`Temporary failure resolving 'azure.archive.ubuntu.com'`，会让 apt 装包整体失败），
# 此时回退到**同版本**（21.12）官方预编译包，装到 /usr/local。
#
# 两条路都失败即退出 1：不允许用「没装 protoc」把 descriptor 校验伪装成通过。
set -euo pipefail

# 与 ubuntu-24.04 的 protobuf-compiler 同版本：回退路径生成的 descriptor 与 apt
# 安装一致，避免 CI 与本地因 protoc 版本不同而产出不同字节。
readonly PROTOC_VERSION="21.12"
readonly SHA256_X86_64="3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8"
readonly SHA256_AARCH64="2dd17f75d66a682640b136e31848da9fb2eefe68d55303baf8b32617374f6711"

SUDO=()
if [ "$(id -u)" -ne 0 ]; then
    SUDO=(sudo)
fi
readonly SUDO

if command -v protoc >/dev/null 2>&1 && protoc --version >/dev/null 2>&1; then
    echo "protoc already available: $(protoc --version)"
    exit 0
fi

if timeout --kill-after=10 600 "${SUDO[@]}" apt-get update \
        -o Acquire::Retries=3 -o Acquire::http::Timeout=20 \
        -o Acquire::https::Timeout=20 \
    && timeout --kill-after=10 600 "${SUDO[@]}" env DEBIAN_FRONTEND=noninteractive \
        apt-get install -y protobuf-compiler; then
    echo "protoc installed via apt: $(protoc --version)"
    exit 0
fi

echo "::warning::apt protobuf-compiler failed; falling back to the pinned protoc release archive" >&2

case "$(uname -m)" in
    x86_64)
        asset="protoc-${PROTOC_VERSION}-linux-x86_64.zip"
        expected_sha256="${SHA256_X86_64}"
        ;;
    aarch64)
        asset="protoc-${PROTOC_VERSION}-linux-aarch_64.zip"
        expected_sha256="${SHA256_AARCH64}"
        ;;
    *)
        echo "unsupported architecture for the protoc fallback: $(uname -m)" >&2
        exit 1
        ;;
esac

readonly asset
readonly expected_sha256
url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${asset}"

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

curl -fsSL --retry 3 --retry-delay 5 --max-time 180 -o "${tmp}/${asset}" "${url}"
echo "${expected_sha256}  ${tmp}/${asset}" | sha256sum -c -
unzip -q "${tmp}/${asset}" -d "${tmp}/protoc"
"${SUDO[@]}" install -m 0755 "${tmp}/protoc/bin/protoc" /usr/local/bin/protoc
# 官方 zip 带 well-known types：prost-build 解析 `google/protobuf/*.proto` 需要它。
"${SUDO[@]}" cp -R "${tmp}/protoc/include/." /usr/local/include/
hash -r
# 用刚装好的二进制自证：PATH 前面若还有别的 protoc，`protoc --version` 证明不了
# 这次回退装成功了。GitHub runner 上 /usr/local/bin 本来就优先于 /usr/bin。
/usr/local/bin/protoc --version
