#!/usr/bin/env bash
# ============================================================================
# common.sh - shared provisioning for every Jepsen VM
#   * switch apt to a domestic (China) mirror
#   * install base packages
#   * append the lab /etc/hosts entries
# Idempotent; safe to re-run.
# ============================================================================
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

APT_MIRROR="${JEPSEN_APT_MIRROR:-mirrors.aliyun.com}"

# Debian codename (bookworm, trixie, ...)
CODENAME="$(. /etc/os-release && echo "${VERSION_CODENAME:-$(echo "$VERSION" | sed -E 's/.*\(([^)]+)\).*/\1/')}")"

echo ">> [common] apt mirror: ${APT_MIRROR} (codename=${CODENAME})"

# --- swap existing apt sources for the domestic mirror (deb822 format) -------
mkdir -p /etc/apt/sources.list.d.bak
for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
    if [ -f "$f" ]; then
        mv "$f" "/etc/apt/sources.list.d.bak/$(basename "$f").bak" || true
    fi
done

cat > /etc/apt/sources.list.d/jepsen.sources <<EOF
Types: deb
URIs: https://${APT_MIRROR}/debian
Suites: ${CODENAME} ${CODENAME}-updates
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg

Types: deb
URIs: https://${APT_MIRROR}/debian-security
Suites: ${CODENAME}-security
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg
EOF

apt-get update -y

# --- base packages -----------------------------------------------------------
apt-get install -y --no-install-recommends \
    ca-certificates curl wget gnupg \
    openssh-client iputils-ping dnsutils \
    procps less vim-tiny

# --- lab /etc/hosts entries --------------------------------------------------
if [ -n "${HOSTS_ENTRIES:-}" ]; then
    if ! grep -q "jepsen lab" /etc/hosts; then
        printf '\n# jepsen lab\n%s\n' "${HOSTS_ENTRIES}" >> /etc/hosts
    fi
fi

echo ">> [common] done"
