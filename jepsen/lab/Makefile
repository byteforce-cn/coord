# ============================================================================
#  coord in-repo Jepsen lab Makefile (方案 D)
#
#  Versioned INSIDE the coord repository at jepsen/lab/, so a fresh clone of
#  coord already contains everything needed to boot a Jepsen lab and run the
#  coord tests against that same checkout.
#
#  Two providers behind one interface (JEPSEN_PROVIDER, default vagrant):
#    * vagrant — VirtualBox/libvirt VMs (control + n1..nN). Full fidelity;
#                recommended for the full matrix and the 72h soak.
#    * docker  — the jepsen-docker images via compose (only Docker needed);
#                daily dev / smoke / CI. control image already has the jepsen
#                0.3.14-SNAPSHOT library lein-installed.
#
#  Self-relative paths: REPO_ROOT / TEST_DIR / COORD_BIN are derived from this
#  Makefile's own location, so there is no dependency on where coord or the
#  Jepsen library happen to be cloned on the host. The Jepsen *library* is
#  cloned inside the control VM by provision/control.sh (Vagrant provider) —
#  no host-side jepsen clone is required.
#
#  Both providers share a VM-identical control-side layout (/root/coord-test,
#  /root/nodes, /root/.ssh/id_ed25519), so coord-soak.sh / db.clj defaults
#  work unchanged. `make docker-upload` creates that layout inside the
#  control container from the bind-mounted repo.
#
#  Common usage:
#    make up                # boot the lab (provider default: vagrant)
#    make setup             # one-time: lein install of the local jepsen lib
#    make upload            # deploy coord/jepsen + built coord binary to control
#    make smoke             # noop smoke test (no DB)
#    make quick             # 20s coord sanity run (no nemesis)
#    make test              # coord test (params below)
#    make matrix            # full regression matrix (workloads x nemeses)
#    make soak              # 72h soak (background on control)
#    make soak-status       # progress
#    make soak-tail         # tail the soak log
#    make soak-stop         # cancel the soak
#
#  Docker flavour (shorthand for JEPSEN_PROVIDER=docker):
#    make docker-up docker-upload docker-quick docker-test docker-matrix ...
#
#  Overridable variables (e.g. `make test NEMESIS=kill TIME_LIMIT=120`):
#    JEPSEN_PROVIDER (vagrant|docker), WORKLOAD, NEMESIS, TIME_LIMIT,
#    CONCURRENCY, RATE, CHECKER, MATRIX_TIME, SOAK_HOURS, SOAK_RATE,
#    SOAK_QUIET, SOAK_DISRUPT, NAME
# ============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# --- self-relative layout (robust to any invocation directory) ---------------
LAB_DIR   := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
# coord repo root
REPO_ROOT := $(abspath $(LAB_DIR)/../..)
# coord/jepsen (canonical test project)
TEST_DIR  := $(abspath $(LAB_DIR)/..)
COORD_BIN := $(REPO_ROOT)/target/release/coord

# --- provider: vagrant | docker --------------------------------------------------
JEPSEN_PROVIDER ?= vagrant

# --- transports ---------------------------------------------------------------
CO     := docker compose -f $(LAB_DIR)/docker/docker-compose.lab.yml
VG_SSH := cd $(LAB_DIR) && vagrant ssh control
VG     := cd $(LAB_DIR) && vagrant

# Run a command as root on the control node. IMPORTANT: the whole remote
# command must be a SINGLE quoted argument (vagrant joins args without
# re-quoting). In docker mode the same command runs via `compose exec`.
define runctl
$(if $(filter docker,$(JEPSEN_PROVIDER)),$(CO) exec -T control bash -c "$(1)",$(VG_SSH) -- 'sudo -i bash -lc "$(1)"')
endef

# --- coord test ---------------------------------------------------------------
CTRL_PROJECT := /root/coord-test
# The lab control node has no outbound network (containers are isolated; the
# vagrant VMs may also be offline). Without `-o` leiningen blocks for minutes
# on dependency resolution for artifacts that are already in ~/.m2, and every
# `make quick` / `make checkers` looks like a hang. Offline mode is therefore
# the default; ONLINE=1 restores the online behaviour when you really need it.
LEIN_OFFLINE := $(if $(ONLINE),,-o )
LEIN_RUN     := LEIN_ROOT=true lein $(LEIN_OFFLINE)run test \
                --nodes-file /root/nodes \
                --username root \
                --ssh-private-key /root/.ssh/id_ed25519
UPLOAD_STAGE := /tmp/coord-upload

WORKLOAD    := register
NEMESIS     := partition-halves
TIME_LIMIT  := 60
CONCURRENCY := 1n
RATE        :=
CHECKER     :=
MATRIX_TIME := 45
MATRIX      := register:none      register:kill      register:kill-all \
               register:pause     register:partition register:partition-halves \
               register:partition-ring register:all \
               cas-register:none  cas-register:kill  cas-register:pause \
               cas-register:partition cas-register:partition-halves \
               cas-register:partition-ring

# --- 72h soak ---------------------------------------------------------------
SOAK_HOURS      := 72
SOAK_TIME_LIMIT :=      # set to run a short soak (e.g. 120) instead of SOAK_HOURS
SOAK_RATE       := 0.5
SOAK_QUIET      := 1800
SOAK_DISRUPT    := 600
SOAK_REGIONS    :=      # multi-raft mode: region count (use WORKLOAD=multi-register)

# T5.20（R-MR-10）：multi-register / multi-region 档位。
#   make matrix-multi          # multi-register x 6 nemeses, --regions $(MULTI_REGIONS)
#   make test WORKLOAD=multi-register REGIONS=3 NEMESIS=partition-halves
# 说明：--regions N 走 Multi-Raft（[multi_raft] 装配）；缺省（REGIONS 空）= legacy 单 Raft。
REGIONS         :=      # 透传给 coord.clj --regions（make test / matrix-multi）
MULTI_REGIONS   := 3

# T0.5 随机种子（缺省由 coord.clj 生成并打印，同时写进 test map / results.edn）
SEED            :=
# T0.6 nemesis 抖动：默认开（3–8s 短跑 / ±20% soak）；设 NO_JITTER=1 回到固定节拍
NO_JITTER       :=
# T0.2 门槛覆盖（默认值见 jepsen/docs/dev.md §5.4；验收级 run 必须用确认值）
RTO_MAX          :=
QUIET_MIN_SAMPLE :=
QUIET_AVAIL_MIN  :=
MIN_OP_OK_RATIO  :=
MIN_OP_SAMPLE    :=
# T1.1/T1.5 map 面对参数（§5.1 delete 样本门槛、F3 值大小扫描）
MAP_KEYS         :=
MAP_MIN_DELETES  :=
VALUE_SIZE       :=
# T1.5 mixture：map:txn:scan 的**目标 op 份额**（默认 4,2,2 = 50%/25%/25%）
MIXTURE_RATIO    :=
# T2.1 watch（覆盖缺口 A5）：语义档 / 会话窗口 / 样本门槛 / 会话内重开次数
WATCH_SEMANTICS  :=
WATCH_WINDOW_MS  :=
WATCH_MIN_EVENTS :=
WATCH_RESUMES    :=
# T2.2 lease（覆盖缺口 A6）：TTL / grace / 续期窗口 / Revoke TTL / §5.1 样本门槛 / 安全容差
LEASE_TTL_SECONDS      :=
LEASE_GRACE_MS         :=
LEASE_KEEPALIVE_MS     :=
LEASE_REVOKE_TTL_SECONDS :=
LEASE_MIN_GRANTS       :=
LEASE_MIN_EXPIRIES     :=
LEASE_TOLERANCE_MS     :=
# T1.4 幂等专项：重放前延迟（推到 kill/重启之后，F-03 跨节点分支用）
IDEM_REPLAY_DELAY_MS :=
# T1.4：重放首次尝试的节点偏移（仅影响起点，单独用它复现不了跨节点重放）
IDEM_REPLAY_NODE_OFFSET :=
# T5.1+: 经 agent 代理全部 KV 流量（D1；agent 监听在每台节点上）
VIA_AGENT        :=
AGENT_PORT       :=
# 跳过 fixture 门槛（只用于调试 fixture 本身 —— 正式 run 不要设）
SKIP_CHECKERS    :=
# T6.1：--extra 是 soak 启动器的通用透传（原样拼进 lein 命令），用于新选项
SOAK_EXTRA       :=
# T6.1：soakfull 的面比例（`make soakfull SOAK_MIX=map=40,...`）
SOAK_MIX         :=
# T0.4：env-reset 时用 --strict-clock（找不到同步工具即失败，而不是 WARN）
STRICT_CLOCK     :=

CTRL_FLAGS := $(if $(SEED),--seed $(SEED)) $(if $(NO_JITTER),--no-jitter) \
              $(if $(RTO_MAX),--soak-max-rto-seconds $(RTO_MAX)) \
              $(if $(QUIET_MIN_SAMPLE),--quiet-min-sample $(QUIET_MIN_SAMPLE)) \
              $(if $(QUIET_AVAIL_MIN),--quiet-availability-min $(QUIET_AVAIL_MIN)) \
              $(if $(MIN_OP_OK_RATIO),--min-op-ok-ratio $(MIN_OP_OK_RATIO)) \
              $(if $(MIN_OP_SAMPLE),--min-op-sample $(MIN_OP_SAMPLE)) \
              $(if $(IDEM_REPLAY_DELAY_MS),--idem-replay-delay-ms $(IDEM_REPLAY_DELAY_MS)) \
              $(if $(IDEM_REPLAY_NODE_OFFSET),--idem-replay-node-offset $(IDEM_REPLAY_NODE_OFFSET)) \
              $(if $(MAP_KEYS),--map-keys $(MAP_KEYS)) \
              $(if $(VALUE_SIZE),--value-size $(VALUE_SIZE)) \
              $(if $(MAP_MIN_DELETES),--map-min-deletes $(MAP_MIN_DELETES)) \
              $(if $(MIXTURE_RATIO),--mixture-ratio $(MIXTURE_RATIO)) \
              $(if $(WATCH_SEMANTICS),--watch-semantics $(WATCH_SEMANTICS)) \
              $(if $(WATCH_WINDOW_MS),--watch-window-ms $(WATCH_WINDOW_MS)) \
              $(if $(WATCH_MIN_EVENTS),--watch-min-events $(WATCH_MIN_EVENTS)) \
              $(if $(WATCH_RESUMES),--watch-resumes $(WATCH_RESUMES)) \
              $(if $(LEASE_TTL_SECONDS),--lease-ttl-seconds $(LEASE_TTL_SECONDS)) \
              $(if $(LEASE_GRACE_MS),--lease-grace-ms $(LEASE_GRACE_MS)) \
              $(if $(LEASE_KEEPALIVE_MS),--lease-keepalive-ms $(LEASE_KEEPALIVE_MS)) \
              $(if $(LEASE_REVOKE_TTL_SECONDS),--lease-revoke-ttl-seconds $(LEASE_REVOKE_TTL_SECONDS)) \
              $(if $(LEASE_MIN_GRANTS),--lease-min-grants $(LEASE_MIN_GRANTS)) \
              $(if $(LEASE_MIN_EXPIRIES),--lease-min-expiries $(LEASE_MIN_EXPIRIES)) \
              $(if $(LEASE_TOLERANCE_MS),--lease-tolerance-ms $(LEASE_TOLERANCE_MS)) \
              $(if $(VIA_AGENT),--via-agent) \
              $(if $(AGENT_PORT),--agent-port $(AGENT_PORT))
# fixture 门槛（T0.3）：fixture 红就拒绝起跑（§0-1：没有负控制的 checker 不算完成）
CHECKER_GATE := $(if $(SKIP_CHECKERS),,checkers)
MULTI_MATRIX    := multi-register:none      multi-register:kill \
                   multi-register:pause     multi-register:partition \
                   multi-register:partition-halves multi-register:partition-ring

# M1 数据面回归矩阵（T1.1/T1.2/T1.3/T1.4，每个 45s）。CI/PR 级的「一次过」
# 门禁：`make matrix-m1`；idempotency 在 F-01/F-02 修完并回归后加入（修复前
# 它注定红，按 §6 曾单独作为「证据 run」跑）。
MATRIX_M1       := map:none   map:kill \
                   txn:none   txn:kill \
                   scan:none  scan:kill \
                   mixture:none mixture:kill \
                   idempotency:none

# M2 Watch+Lease 矩阵（T2.1/T2.2，每个 45s）：none 是对照，kill/pause/
# partition-halves 是「流中断 → 按契约 resume」与「Lease 到期/续期在故障下
# 仍成立」的证伪路径。
MATRIX_M2       := watch:none watch:kill watch:pause watch:partition-halves \
                   lease:none lease:kill lease:pause lease:partition-halves

# M2 的 lease 档：45s 短跑的 §5.1 样本门槛（**保守**取值 —— 短跑里
# concurrency 1n × 每 op ~3–6s 只能拿到十几个到期场景；M2 收口的 2h 浸泡与
# 验收 run 用 §5.1 的 100/30，见 dev.md §5.1）。门槛存在的意义是「不能一条
# lease op 都没跑完还判绿」，不是短跑的性能指标。
M2_LEASE_MIN_GRANTS   := 5
M2_LEASE_MIN_EXPIRIES := 2

.PHONY: help info keys binary up halt destroy status provision reload \
        ssh-control ssh-node ping \
        setup stage upload smoke quick test matrix matrix-multi matrix-m1 \
        matrix-m2 soak soakfull soak-status soak-tail soak-results soak-stop \
        soak-wait nightly \
        checkers env-reset \
        soak soak-status soak-tail soak-stop soak-results soak-diag logs \
        docker-%

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'
	@echo
	@echo "  provider: JEPSEN_PROVIDER=vagrant|docker (default vagrant);"
	@echo "            any target also works as docker-<target> shorthand."

info: ## Show active provider + layout + tool availability
	@echo "provider     : $(JEPSEN_PROVIDER)"
	@echo "lab dir      : $(LAB_DIR)"
	@echo "repo root    : $(REPO_ROOT)"
	@echo "test project : $(TEST_DIR)"
	@echo "coord binary : $(COORD_BIN)"
	@if [ "$(JEPSEN_PROVIDER)" = docker ]; then \
	  docker --version && docker compose version; \
	else \
	  $(VG) --version | head -1; \
	fi

# ---------------------------------------------------------------------------
# Lab lifecycle (provider-aware: vagrant VMs or docker compose)
# ---------------------------------------------------------------------------

keys: ## Generate the lab ssh keypair (keys/, never committed; auto in `make up`)
	@test -f "$(LAB_DIR)/keys/jepsen_ed25519" && test -f "$(LAB_DIR)/keys/jepsen_ed25519.pub" \
	  || ssh-keygen -t ed25519 -N '' -f "$(LAB_DIR)/keys/jepsen_ed25519" -q
	@echo "lab ssh keypair ready: $(LAB_DIR)/keys/jepsen_ed25519(.pub)"

binary: ## Build the coord release binary if missing (target/release/coord)
	@test -x "$(COORD_BIN)" || { echo ">> building coord release (missing $(COORD_BIN)) ..."; cd $(REPO_ROOT) && cargo build --release -p coord; }
	@echo "coord binary: $(COORD_BIN) ($$(sha256sum $(COORD_BIN) | cut -c1-8))"

up: ## Boot + provision the lab (vagrant: VMs; docker: compose up)
ifeq ($(JEPSEN_PROVIDER),docker)
	$(CO) up -d
else
	$(MAKE) keys
	$(VG) up
endif

halt: ## Stop the lab (vagrant halt / docker compose stop)
ifeq ($(JEPSEN_PROVIDER),docker)
	$(CO) stop
else
	$(VG) halt
endif

destroy: ## Destroy the lab (vagrant destroy -f / docker compose down -v)
ifeq ($(JEPSEN_PROVIDER),docker)
	$(CO) down -v
else
	$(VG) destroy -f
endif

status: ## Show lab status (vagrant status / docker compose ps)
ifeq ($(JEPSEN_PROVIDER),docker)
	$(CO) ps
else
	$(VG) status
endif

provision: ## (vagrant) Re-provision VMs (all, or NAME=n1 for one); docker: no-op
ifeq ($(JEPSEN_PROVIDER),docker)
	@echo "docker provider has no provision step — container layout is prepared by 'make upload'"
else
	$(VG) provision $(if $(NAME),$(NAME))
endif

reload: ## (vagrant) Reload VMs (applies memory/CPU changes; NAME=n1 for one)
	$(VG) reload $(if $(NAME),$(NAME))

ssh-control: ## (vagrant) Open a root shell on the control node
	$(VG_SSH) -- sudo -i

ssh-node: ## (vagrant) Open a shell on a node (NAME=n1)
	$(VG) ssh $(NAME)

ping: ## Quick reachability check of the control node
	$(call runctl, echo control-ok && free -h | head -2)

# ---------------------------------------------------------------------------
# Setup / deploy
# ---------------------------------------------------------------------------

setup: ## One-time: lein install of the local jepsen lib (vagrant; docker: no-op)
ifeq ($(JEPSEN_PROVIDER),docker)
	@echo "docker: jepsen 0.3.14-SNAPSHOT is already lein-installed in the control image — nothing to do"
else
	$(call runctl, d=/jepsen/jepsen/jepsen; [ -f "$$d/project.clj" ] || d=/jepsen/jepsen; cd "$$d" && lein install)
endif

stage: binary ## Assemble $(UPLOAD_STAGE)/coord-test from jepsen/ + binary (no deploy; vagrant)
	@rm -rf $(UPLOAD_STAGE) && mkdir -p $(UPLOAD_STAGE)/coord-test
	@cp -r $(TEST_DIR)/. $(UPLOAD_STAGE)/coord-test/
	@rm -rf $(UPLOAD_STAGE)/coord-test/store $(UPLOAD_STAGE)/coord-test/soak
	@rm -f $(UPLOAD_STAGE)/coord-test/coord
	@cp $(COORD_BIN) $(UPLOAD_STAGE)/coord-test/coord
	@cp $(UPLOAD_STAGE)/coord-test/scripts/coord-soak.sh $(UPLOAD_STAGE)/coord-test/coord-soak.sh
	@echo "staged -> $(UPLOAD_STAGE)/coord-test (coord @ $$(cd $(REPO_ROOT) && git rev-parse --short HEAD))"

upload: ## Deploy coord/jepsen + built binary to the control node (refuses if soak running)
ifeq ($(JEPSEN_PROVIDER),docker)
	@$(call runctl, for i in {1..60}; do [ -f /root/.ssh/id_rsa ] && [ -f /jepsen/nodes ] && break; sleep 2; done; \
	    if test -x $(CTRL_PROJECT)/scripts/coord-soak.sh; then (cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh check); fi; \
	    ln -sfn /opt/coord/jepsen /root/coord-test && \
	    cp -f /root/.ssh/id_rsa /root/.ssh/id_ed25519 && \
	    cp -f /root/.ssh/id_rsa.pub /root/.ssh/id_ed25519.pub && \
	    ln -sfn /jepsen/nodes /root/nodes)
	@$(MAKE) binary
	@ln -sfn ../target/release/coord $(TEST_DIR)/coord
	@$(call runctl, cd $(CTRL_PROJECT) && chmod +x scripts/coord-soak.sh)
	@echo "docker: repo bind-mounted at /opt/coord; /root/coord-test -> /opt/coord/jepsen"
	@echo "coord @ $$(cd $(REPO_ROOT) && git rev-parse --short HEAD) (bin $$(sha256sum $(COORD_BIN) | cut -c1-8))"
else
	@$(call runctl, if test -x $(CTRL_PROJECT)/scripts/coord-soak.sh; then $(CTRL_PROJECT)/scripts/coord-soak.sh check; fi)
	$(MAKE) stage
	$(VG) upload $(UPLOAD_STAGE)/coord-test /tmp/coord-test control
	$(call runctl, rm -rf $(CTRL_PROJECT) && mv /tmp/coord-test $(CTRL_PROJECT) \
	    && chmod +x $(CTRL_PROJECT)/coord-soak.sh $(CTRL_PROJECT)/scripts/coord-soak.sh)
	@echo "deployed coord @ $$(cd $(REPO_ROOT) && git rev-parse --short HEAD) (bin $$(sha256sum $(COORD_BIN) | cut -c1-8)) -> $(CTRL_PROJECT)"
endif

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

smoke: ## Noop smoke test (framework only, no DB)
ifeq ($(JEPSEN_PROVIDER),docker)
	$(call runctl, cd /opt/coord/jepsen/lab/examples/noop-test \
	    && LEIN_ROOT=true lein $(LEIN_OFFLINE)run test --nodes-file /root/nodes \
	       --username root --ssh-private-key /root/.ssh/id_ed25519 --time-limit 20)
else
	$(VG) upload $(LAB_DIR)/examples/noop-test /tmp/noop-test control
	$(call runctl, rm -rf /root/noop-test && mv /tmp/noop-test /root/noop-test)
	$(call runctl, cd /root/noop-test \
	    && LEIN_ROOT=true lein $(LEIN_OFFLINE)run test --nodes-file /root/nodes \
	       --username root --time-limit 20)
endif

quick: $(CHECKER_GATE) ## 20s coord sanity run (no nemesis)
	$(call runctl, cd $(CTRL_PROJECT) && bash scripts/env-reset-all.sh --wipe-data /var/lib/coord)
	$(call runctl, cd $(CTRL_PROJECT) && $(LEIN_RUN) \
	    --workload register --nemesis none --time-limit 20 --concurrency 1n \
	    $(CTRL_FLAGS))

test: $(CHECKER_GATE) ## Coord test: WORKLOAD, NEMESIS, TIME_LIMIT, CONCURRENCY, RATE, CHECKER, REGIONS
	$(call runctl, cd $(CTRL_PROJECT) && $(LEIN_RUN) \
	    --workload $(WORKLOAD) --nemesis $(NEMESIS) \
	    --time-limit $(TIME_LIMIT) --concurrency $(CONCURRENCY) \
	    $(if $(RATE),--rate $(RATE)) $(if $(CHECKER),--checker $(CHECKER)) \
	    $(if $(REGIONS),--regions $(REGIONS)) $(CTRL_FLAGS))

checkers: ## T0.3: run every checker fixture suite on control (red refuses to start a run)
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.soak scripts/soak-checker-fixtures)
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.gates scripts/gates-fixtures \
	    '{:quiet-min-sample 10 :nemesis :kill}')
	# F-09 回归：门槛必须走**生产默认值**（不传 quiet-* 选项，等价于 coord.clj
	# 传入 nil 的那些键）。用 `:or` 解析的话这里会因为 nil 门槛而 NPE/静默失效。
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.gates scripts/gates-fixtures-defaults \
	    '{:nemesis :kill}')
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.idem scripts/idem-fixtures \
	    '{:min-replay-attempts 0}')
	# T1.1（map/delete）：走 O(n) 写索引路径
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.mapck scripts/map-fixtures \
	    '{:mode :index}')
	# T1.1 的 §5.1 样本门槛（delete ≥ 门槛数）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.mapck scripts/map-fixtures-min-deletes \
	    '{:mode :index :min-deletes 5}')
	# T1.2（txn 全形态）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.txnck scripts/txn-fixtures)
	# T1.3（scan / revision 读）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.scanck scripts/scan-fixtures)
	# T1.5（mixture：组合 checker —— 三面判据的**穿透** + 路由/样本门槛）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.mixck scripts/mixture-fixtures \
	    '{:map-mode :index :min-sample 1}')
	# T1.5 的门槛档：把「每个面都必须真的跑到」单独钉住（:min-sample 3）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.mixck scripts/mixture-fixtures-routing \
	    '{:map-mode :index :min-sample 3}')
	# T2.1（watch：结构/续传起点/fabricated/静默丢事件）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.watchck scripts/watch-fixtures \
	    '{:min-events 1}')
	# T2.1 的样本门槛（§5.1「每 watcher 事件 ≥ 200」的 fixture 档）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.watchck scripts/watch-fixtures-sample \
	    '{:min-events 5}')
	# T2.1 的另外两个语义档：**同一条历史、期望值相反**，证明参数真的接上了
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.watchck scripts/watch-fixtures-lossless \
	    '{:min-events 1 :semantics :lossless}')
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.watchck scripts/watch-fixtures-coalescing \
	    '{:min-events 1 :semantics :coalescing}')
	# T2.2（lease：安全 / 活性 / 级联删除 / 续期 / 续期后 ttl=0 / 读失败不算消失）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.leaseck scripts/lease-fixtures)
	# T2.2 的 §5.1 样本门槛（§5.1「grant ≥ 100；到期场景 ≥ 30」的 fixture 档）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.leaseck scripts/lease-fixtures-sample \
	    '{:min-grants 3 :min-expiries 2}')
	# T6.1（soak-full 五面组合：证明 watch/lease 能进组合，且**未声明面**会被抓到）
	$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
	    scripts/run-checker-tests.clj jepsen.coord.mixck scripts/soakfull-fixtures \
	    '{:surfaces [:map :txn :scan :watch :lease] :min-sample 1 :watch-min-events 1 :min-grants 1 :min-expiries 1}')

env-reset: ## T0.4: idempotent environment preflight on every node (wipes node data dirs)
	$(call runctl, cd $(CTRL_PROJECT) && bash scripts/env-reset-all.sh \
	    --wipe-data /var/lib/coord $(if $(STRICT_CLOCK),--strict-clock,))

matrix: checkers ## Full regression matrix (workloads x nemeses), stops on first failure
	@set -e; for combo in $(MATRIX); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL MATRIX PASSED ====="

matrix-multi: checkers ## T5.20（R-MR-10）multi-register 全矩阵（--regions $(MULTI_REGIONS)）	@set -e; for combo in $(MULTI_MATRIX); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n (regions=$(MULTI_REGIONS)) ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=1n REGIONS=$(MULTI_REGIONS) SKIP_CHECKERS=1 \
	           JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "MULTI MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL MULTI-REGISTER MATRIX PASSED ====="

matrix-m1: checkers ## M1 数据面回归矩阵（T1.1 map / T1.2 txn / T1.3 scan / T1.5 mixture / T1.4 idempotency）
	@set -e; for combo in $(MATRIX_M1); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=2n SKIP_CHECKERS=1 \
	           JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "M1 MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL M1 MATRIX PASSED ====="

matrix-m2: checkers ## M2 Watch+Lease 矩阵（T2.1 watch / T2.2 lease × none|kill|pause|partition-halves，45s）
	@set -e; for combo in $(MATRIX_M2); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=1n SKIP_CHECKERS=1 \
	           LEASE_MIN_GRANTS=$(M2_LEASE_MIN_GRANTS) \
	           LEASE_MIN_EXPIRIES=$(M2_LEASE_MIN_EXPIRIES) \
	           JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "M2 MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL M2 MATRIX PASSED ====="

# ---------------------------------------------------------------------------
# Soak (72h)
# ---------------------------------------------------------------------------

soak: ## Start the soak in the background on control (SOAK_HOURS / SOAK_TIME_LIMIT, SOAK_RATE, SOAK_REGIONS...)
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh start \
	    $(if $(SOAK_TIME_LIMIT),--time-limit $(SOAK_TIME_LIMIT),--hours $(SOAK_HOURS)) \
	    --rate $(SOAK_RATE) --quiet $(SOAK_QUIET) --disrupt $(SOAK_DISRUPT) \
	    $(if $(SOAK_REGIONS),--regions $(SOAK_REGIONS)) \
	    --concurrency $(CONCURRENCY) \
	    $(if $(SEED),--seed $(SEED)) \
	    $(if $(MIXTURE_RATIO),--mixture-ratio $(MIXTURE_RATIO)) \
	    $(if $(MAP_MIN_DELETES),--map-min-deletes $(MAP_MIN_DELETES)) \
	    $(if $(SOAK_EXTRA),--extra '$(SOAK_EXTRA)') \
	    --workload $(WORKLOAD))
	@echo "Soak started. Monitor: make soak-status   (or make soak-tail)"

soakfull: ## T6.1: 组合浸泡（默认 --workload soakfull，面比例由 SOAK_MIX 控制；不跑 72h 请设 SOAK_TIME_LIMIT）
	$(MAKE) soak WORKLOAD=soakfull \
	    SOAK_MIX='$(SOAK_MIX)' \
	    SOAK_EXTRA="$(SOAK_EXTRA) $(if $(SOAK_MIX),--soak-mix $(SOAK_MIX),)" \
	    --no-print-directory

soak-status: ## Soak progress + latest log lines
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh status)

soak-wait: ## Block until the detached soak finishes (used by `make nightly`)
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh wait $(SOAK_WAIT_INTERVAL))

nightly: ## T6.x: lab 侧夜间门禁（checkers -> matrix-m1 -> matrix-m2 -> soakfull + 等待 + 结果）
	@bash ../scripts/nightly-soak-gates.sh

soak-tail: ## Tail the soak log (last 30 lines)
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh tail)

soak-stop: ## Cancel the soak run
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh stop)

soak-results: ## Summarize the latest soak results.edn
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh results)

soak-diag: ## Run one-shot all-node diagnostics (n1..n5) on control
ifeq ($(JEPSEN_PROVIDER),docker)
	$(call runctl, bash /opt/coord/jepsen/scripts/soak-diag.sh)
else
	$(VG) upload $(TEST_DIR)/scripts/soak-diag.sh /tmp/soak-diag.sh control
	$(call runctl, bash /tmp/soak-diag.sh)
endif

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

logs: ## Tail the latest test's jepsen.log (via coord-soak.sh log)
	$(call runctl, $(CTRL_PROJECT)/scripts/coord-soak.sh log)

# ---------------------------------------------------------------------------
# Docker shorthand: `make docker-<target>` == `make <target> JEPSEN_PROVIDER=docker`
# ---------------------------------------------------------------------------
docker-%:
	$(MAKE) $* JEPSEN_PROVIDER=docker --no-print-directory
