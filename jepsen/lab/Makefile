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
LEIN_RUN     := LEIN_ROOT=true lein run test \
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
MULTI_MATRIX    := multi-register:none      multi-register:kill \
                   multi-register:pause     multi-register:partition \
                   multi-register:partition-halves multi-register:partition-ring

.PHONY: help info keys binary up halt destroy status provision reload \
        ssh-control ssh-node ping \
        setup stage upload smoke quick test matrix matrix-multi \
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
	    && LEIN_ROOT=true lein run test --nodes-file /root/nodes \
	       --username root --ssh-private-key /root/.ssh/id_ed25519 --time-limit 20)
else
	$(VG) upload $(LAB_DIR)/examples/noop-test /tmp/noop-test control
	$(call runctl, rm -rf /root/noop-test && mv /tmp/noop-test /root/noop-test)
	$(call runctl, cd /root/noop-test \
	    && LEIN_ROOT=true lein run test --nodes-file /root/nodes \
	       --username root --time-limit 20)
endif

quick: ## 20s coord sanity run (no nemesis)
	$(call runctl, cd $(CTRL_PROJECT) && $(LEIN_RUN) \
	    --workload register --nemesis none --time-limit 20 --concurrency 1n)

test: ## Coord test: WORKLOAD, NEMESIS, TIME_LIMIT, CONCURRENCY, RATE, CHECKER, REGIONS
	$(call runctl, cd $(CTRL_PROJECT) && $(LEIN_RUN) \
	    --workload $(WORKLOAD) --nemesis $(NEMESIS) \
	    --time-limit $(TIME_LIMIT) --concurrency $(CONCURRENCY) \
	    $(if $(RATE),--rate $(RATE)) $(if $(CHECKER),--checker $(CHECKER)) \
	    $(if $(REGIONS),--regions $(REGIONS)))

matrix: ## Full regression matrix (workloads x nemeses), stops on first failure
	@set -e; for combo in $(MATRIX); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=1n JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL MATRIX PASSED ====="

matrix-multi: ## T5.20（R-MR-10）multi-register 全矩阵（--regions $(MULTI_REGIONS)）
	@set -e; for combo in $(MULTI_MATRIX); do \
	  w=$${combo%%:*}; n=$${combo##*:}; \
	  echo; echo "===== $$w / $$n (regions=$(MULTI_REGIONS)) ====="; \
	  $(MAKE) test WORKLOAD=$$w NEMESIS=$$n TIME_LIMIT=$(MATRIX_TIME) \
	           CONCURRENCY=1n REGIONS=$(MULTI_REGIONS) \
	           JEPSEN_PROVIDER=$(JEPSEN_PROVIDER) --no-print-directory \
	    || { echo "MULTI MATRIX FAILED at $$w/$$n"; exit 1; }; \
	done; echo; echo "===== ALL MULTI-REGISTER MATRIX PASSED ====="

# ---------------------------------------------------------------------------
# Soak (72h)
# ---------------------------------------------------------------------------

soak: ## Start the soak in the background on control (SOAK_HOURS / SOAK_TIME_LIMIT, SOAK_RATE, SOAK_REGIONS...)
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh start \
	    $(if $(SOAK_TIME_LIMIT),--time-limit $(SOAK_TIME_LIMIT),--hours $(SOAK_HOURS)) \
	    --rate $(SOAK_RATE) --quiet $(SOAK_QUIET) --disrupt $(SOAK_DISRUPT) \
	    $(if $(SOAK_REGIONS),--regions $(SOAK_REGIONS)) \
	    --workload $(WORKLOAD))
	@echo "Soak started. Monitor: make soak-status   (or make soak-tail)"

soak-status: ## Soak progress + latest log lines
	$(call runctl, cd $(CTRL_PROJECT) && ./scripts/coord-soak.sh status)

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
