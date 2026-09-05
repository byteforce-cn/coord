#!/usr/bin/env bash
# ============================================================================
# control.sh - Jepsen control node provisioning (in-repo lab: coord/jepsen/lab)
#   * OpenJDK 21      (Tsinghua Adoptium mirror - tarball)
#   * Leiningen 2.12  (script + standalone jar via gh-proxy.com)
#   * lein user profile with domestic mirrors:
#       Maven Central -> Aliyun,  Clojars -> Tencent (+ direct clojars fallback)
#   * lab SSH key so the control node can reach n1..nN as root
#   * /root/nodes file for the jepsen CLI
#   * Jepsen library: cloned INSIDE the VM (no host-side clone needed) when no
#     copy is already present at /jepsen/jepsen — keeps a fresh `coord` clone
#     self-bootstrapping.  Pin the upstream source/ref via JEPSEN_LIB_REPO /
#     JEPSEN_LIB_REF (the coord test project depends on the version this
#     checkout `lein install`s, currently jepsen 0.3.14-SNAPSHOT from main).
#   * pre-fetch jepsen deps (validates the mirror setup)
# Idempotent; safe to re-run.
# ============================================================================
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export LEIN_ROOT=true                      # allow lein to run as root

LEIN_VER="${JEPSEN_LEIN_VER:-2.12.0}"
JDK_MIRROR="${JEPSEN_JDK_MIRROR:-https://mirrors.tuna.tsinghua.edu.cn/Adoptium}"
GH_PROXY="${JEPSEN_GH_PROXY:-https://gh-proxy.com}"
NODES="${JEPSEN_NODES:-5}"
XMX="${JEPSEN_XMX:-12g}"
JEP_REPO="${JEPSEN_LIB_REPO:-https://github.com/jepsen-io/jepsen.git}"
JEP_REF="${JEPSEN_LIB_REF:-main}"

echo ">> [control] installing packages"
apt-get update -y
apt-get install -y --no-install-recommends \
    git gnuplot graphviz libjna-java \
    pssh screen vim htop libfontconfig1 fontconfig

# --- OpenJDK 21 from Tsinghua Adoptium ---------------------------------------
if [ ! -x /opt/jdk-21/bin/java ]; then
    echo ">> [control] downloading OpenJDK 21 from ${JDK_MIRROR}"
    dir_url="${JDK_MIRROR}/21/jdk/x64/linux/"
    tarball="$(curl -fsSL "${dir_url}" | grep -o 'OpenJDK21U-jdk_x64_linux_hotspot_[0-9._]*\.tar\.gz' | sort -V | tail -1)"
    [ -n "${tarball}" ] || { echo "!! cannot find a JDK21 tarball at ${dir_url}"; exit 1; }
    echo ">> [control] fetching ${tarball}"
    curl -fSL --retry 3 -o /tmp/jdk21.tar.gz "${dir_url}${tarball}"
    mkdir -p /opt/jdk-21
    tar -xzf /tmp/jdk21.tar.gz -C /opt/jdk-21 --strip-components=1
    rm -f /tmp/jdk21.tar.gz
fi

cat > /etc/profile.d/jepsen-jdk.sh <<'EOF'
export JAVA_HOME=/opt/jdk-21
export PATH="$JAVA_HOME/bin:$PATH"
EOF

# ensure java is on PATH in any root/vagrant shell, not just login shells
for rc in /root/.bashrc /home/vagrant/.bashrc; do
    if [ -f "$rc" ]; then
        grep -q 'jepsen-jdk.sh' "$rc" || echo '. /etc/profile.d/jepsen-jdk.sh' >> "$rc"
    fi
done

export JAVA_HOME=/opt/jdk-21
export PATH="$JAVA_HOME/bin:/usr/local/bin:$PATH"

# --- Leiningen ----------------------------------------------------------------
mkdir -p /root/.lein/self-installs /root/.ssh
if [ ! -x /usr/local/bin/lein ]; then
    echo ">> [control] downloading lein script via ${GH_PROXY}"
    curl -fSL --retry 3 -o /usr/local/bin/lein \
        "${GH_PROXY}/https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein"
    chmod +x /usr/local/bin/lein
fi

jar="/root/.lein/self-installs/leiningen-${LEIN_VER}-standalone.jar"
if [ ! -f "${jar}" ]; then
    echo ">> [control] downloading leiningen ${LEIN_VER} standalone jar via ${GH_PROXY}"
    curl -fSL --retry 3 -o "${jar}" \
        "${GH_PROXY}/https://github.com/technomancy/leiningen/releases/download/${LEIN_VER}/leiningen-${LEIN_VER}-standalone.jar"
fi

# lein user profile with domestic mirrors (uploaded by the file provisioner)
install -m 600 /tmp/lein-profiles.clj /root/.lein/profiles.clj

# also make lein usable by the default 'vagrant' user (vagrant ssh logs in as
# vagrant) so it does not attempt a doomed GitHub self-install
if [ -d /home/vagrant ]; then
    mkdir -p /home/vagrant/.lein/self-installs
    cp -n "/root/.lein/self-installs/leiningen-${LEIN_VER}-standalone.jar" \
          "/home/vagrant/.lein/self-installs/leiningen-${LEIN_VER}-standalone.jar" || true
    cp -n /root/.lein/profiles.clj /home/vagrant/.lein/profiles.clj || true
    chown -R vagrant:vagrant /home/vagrant/.lein
fi

echo ">> [control] lein version (root):"
lein version

# --- lab SSH key (control -> nodes) -------------------------------------------
chmod 700 /root/.ssh
install -m 600 /tmp/jepsen_id     /root/.ssh/id_ed25519
install -m 644 /tmp/jepsen_id.pub /root/.ssh/id_ed25519.pub

cat > /root/.ssh/config <<EOF
Host n1 n2 n3 n4 n5 control
    User root
    IdentityFile /root/.ssh/id_ed25519
    StrictHostKeyChecking accept-new
    UserKnownHostsFile /root/.ssh/known_hosts
EOF
chmod 600 /root/.ssh/config

# trust node host keys (best effort; nodes may not be up yet on a partial up)
for i in $(seq 1 "${NODES}"); do
    ssh-keyscan -H "n${i}" >> /root/.ssh/known_hosts 2>/dev/null || true
done
ssh-keyscan -H control >> /root/.ssh/known_hosts 2>/dev/null || true
sort -u -o /root/.ssh/known_hosts /root/.ssh/known_hosts 2>/dev/null || true

# --- nodes file for the jepsen CLI --------------------------------------------
{
    for i in $(seq 1 "${NODES}"); do echo "n${i}"; done
} > /root/nodes
echo ">> [control] nodes file: $(tr '\n' ' ' < /root/nodes)"

# --- Jepsen library: ensure a checkout exists in the VM -----------------------
# Layout A (in-VM clone, canonical):  repo at /jepsen/jepsen, lein project at
#   /jepsen/jepsen/jepsen.
# Layout B (host-synced copy, legacy): repo/project synced so the project lands
#   directly at /jepsen/jepsen.
# The helper below resolves whichever is present; both are supported.
#
# A clone is only attempted when NOTHING is present yet, so re-provisioning
# never re-clones (and never touches a host-synced copy).
JEP_PROJ=""
if [ -f /jepsen/jepsen/jepsen/project.clj ]; then
    JEP_PROJ=/jepsen/jepsen/jepsen
elif [ -f /jepsen/jepsen/project.clj ]; then
    JEP_PROJ=/jepsen/jepsen
elif [ ! -d /jepsen/jepsen ]; then
    echo ">> [control] cloning Jepsen ${JEP_REF} from ${JEP_REPO} (no host-side clone needed)"
    mkdir -p /jepsen
    git clone --depth 1 --single-branch --branch "${JEP_REF}" "${JEP_REPO}" /jepsen/jepsen
    JEP_PROJ=/jepsen/jepsen/jepsen
fi

# --- tune jepsen project for the VM memory ------------------------------------
# jepsen's project.clj defaults to -Xmx32g which cannot start on this VM.
# Normalize whatever -Xmx is present to -Xmx${XMX} (idempotent across heap
# changes) so `lein run test` works from the library project too.
if [ -n "${JEP_PROJ}" ] && [ -f "${JEP_PROJ}/project.clj" ] && grep -q '\-Xmx[0-9]' "${JEP_PROJ}/project.clj"; then
    echo ">> [control] setting ${JEP_PROJ} :jvm-opts to -Xmx${XMX}"
    sed -i "s/-Xmx[0-9][0-9]*[gGmM]/-Xmx${XMX}/g" "${JEP_PROJ}/project.clj"
fi

# --- pre-fetch jepsen dependencies through the mirrors ------------------------
if [ -n "${JEP_PROJ}" ] && [ -f "${JEP_PROJ}/project.clj" ]; then
    echo ">> [control] pre-fetching jepsen deps in ${JEP_PROJ} (validates mirror config)"
    cd "${JEP_PROJ}"
    lein deps
fi

echo ">> [control] provisioning done"
echo ">> [control] login :  vagrant ssh control"
echo ">> [control] run   :  cd ${JEP_PROJ:-/jepsen/jepsen} && lein run test --nodes-file /root/nodes --username root ..."
