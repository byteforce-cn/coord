#!/usr/bin/env bash
# ============================================================================
# node.sh - Jepsen DB node provisioning
#   * openssh-server + sudo + iptables (jepsen os.debian relies on these)
#   * root key login using the lab key (control -> node)
# Idempotent; safe to re-run.
# ============================================================================
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo ">> [node] installing packages"
apt-get update -y
apt-get install -y --no-install-recommends \
    openssh-server sudo iptables iproute2 \
    ca-certificates curl ntp procps

# --- allow root login with the lab key ---------------------------------------
mkdir -p /root/.ssh
chmod 700 /root/.ssh
touch /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

if [ -f /tmp/jepsen_id.pub ]; then
    pub="$(cat /tmp/jepsen_id.pub)"
    if ! grep -qF "${pub}" /root/.ssh/authorized_keys; then
        echo "${pub}" >> /root/.ssh/authorized_keys
    fi
fi

# --- sshd: root key login, no password ---------------------------------------
sshd_cfg=/etc/ssh/sshd_config
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' "${sshd_cfg}"
grep -q '^PermitRootLogin' "${sshd_cfg}" || echo 'PermitRootLogin prohibit-password' >> "${sshd_cfg}"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' "${sshd_cfg}"
grep -q '^PasswordAuthentication' "${sshd_cfg}" || echo 'PasswordAuthentication no' >> "${sshd_cfg}"
grep -q '^UseDNS' "${sshd_cfg}" || echo 'UseDNS no' >> "${sshd_cfg}"

systemctl restart ssh 2>/dev/null || systemctl restart sshd 2>/dev/null || service ssh restart || true

echo ">> [node] done"
