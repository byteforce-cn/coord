#!/usr/bin/env bash
# ============================================================================
# jepsen-lab.sh — coord 仓库内 Jepsen lab 引导/自检（fresh-clone 友好）
#
# lab 本体在 coord 仓库 `jepsen/lab/`（Vagrant + Docker 双 provider，随 coord
# 版本化）。本脚本只做一件事：帮你确认宿主机工具链并打印下一步，不执行任何
# 会改动 lab 的动作。
#
# 用法:
#   ./scripts/jepsen-lab.sh            # 打印快速开始（默认）
#   ./scripts/jepsen-lab.sh doctor     # 检查 vagrant/docker 工具可用性
#   ./scripts/jepsen-lab.sh doctor --provider docker   # 只看 docker
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LAB_DIR="${REPO_ROOT}/jepsen/lab"
PROVIDER="${1:-}"

say()  { printf '\033[1;34m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  [ok] %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  [--] %s\033[0m\n' "$*"; }

doctor() {
  say "jepsen-lab doctor (repo: ${REPO_ROOT})"
  [ -d "$LAB_DIR" ] && ok "lab dir present: ${LAB_DIR}" \
                    || { warn "missing lab dir: ${LAB_DIR} (broken checkout?)"; exit 1; }

  local want="${1:-all}"
  if [ "$want" = all ] || [ "$want" = vagrant ]; then
    if command -v vagrant >/dev/null 2>&1; then
      ok "vagrant: $(vagrant --version 2>/dev/null | head -1)"
      command -v VBoxManage >/dev/null 2>&1 \
        && ok "virtualbox: present" \
        || { command -v virsh >/dev/null 2>&1 && ok "libvirt: present" \
             || warn "neither VirtualBox nor libvirt detected (vagrant provider needs one)"; }
    else
      warn "vagrant not found (Vagrant provider unavailable)"
    fi
  fi

  if [ "$want" = all ] || [ "$want" = docker ]; then
    if command -v docker >/dev/null 2>&1; then
      ok "docker: $(docker --version 2>/dev/null | head -1)"
      docker compose version >/dev/null 2>&1 \
        && ok "docker compose: available" \
        || warn "docker compose not available"
    else
      warn "docker not found (Docker provider unavailable)"
    fi
  fi
}

quickstart() {
  say "coord Jepsen lab — quick start (详见 ${LAB_DIR}/README.md)"
  cat <<EOF

  1) 进入 lab：            cd ${LAB_DIR}
  2) 选择 provider：
        vagrant（全矩阵/72h soak 首选，需 VirtualBox/libvirt）：
            make up && make setup && make upload && make quick
        docker（日常开发/冒烟/CI，仅需 Docker）：
            make docker-up && make docker-upload && make docker-quick
  3) 常用：
            make test                  # WORKLOAD/NEMESIS/TIME_LIMIT 可覆盖
            make test NEMESIS=kill TIME_LIMIT=120
            make matrix                # 全矩阵
            make soak                  # 72h soak（后台）
            make soak-status           # soak 进度
  4) 检查宿主机工具：      ./scripts/jepsen-lab.sh doctor
EOF
}

case "${1:-}" in
  doctor)
    shift
    doctor "${1:-all}"
    ;;
  *)
    quickstart
    ;;
esac
