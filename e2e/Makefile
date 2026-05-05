.PHONY: build up up-dev up-ci up-tls tls-certs \
       test test-local test-fast test-single \
       e2e-up e2e-test e2e-smoke e2e-core e2e-failover e2e-feature e2e-full e2e-reset e2e-down \
       down clean logs logs-cluster ps sdk-base jars reload-% help

# ── Compose 入口 ─────────────────────────────────────────────────────────────
# 本地默认：不指定 -f，让 docker-compose.override.yml 自动叠加
COMPOSE     := docker compose
# CI / 完整镜像模式：显式只用基础 compose，禁止 override 叠加
COMPOSE_CI  := docker compose -f docker-compose.yml
# TLS 叠加模式
COMPOSE_TLS := docker compose -f docker-compose.yml -f docker-compose.tls.yml

# ── 本地测试通用参数 ─────────────────────────────────────────────────────────
LOCAL_TEST_PROPS := \
  -Dcoord.grpc.address=localhost:9090 \
  -Dcoord.http.address=http://[::1]:8080 \
  -Dorder.service.url=http://localhost:18080 \
  -Dpay.service.url=http://localhost:18081 \
  -Dinventory.service.url=http://localhost:18082

# ── 环境 profile：single (默认) vs cluster ───────────────────────────────────
SINGLE_SERVICES := coord-1 order-service pay-service inventory-service
CLUSTER_SERVICES := coord-1 coord-2 coord-3 order-service pay-service inventory-service

# ══════════════════════════════════════════════════════════════════════════════
# 推荐本地命令（一眼就懂）
# ══════════════════════════════════════════════════════════════════════════════

## e2e-up — 本地启动可复用环境（单节点 + mock services，override 自动生效）
e2e-up: jars
	$(COMPOSE) up -d $(SINGLE_SERVICES)
	@echo "单节点环境已启动（coord-1 + mock services）。"

## e2e-up-cluster — 本地启动 3 节点集群（failover 场景需要）
e2e-up-cluster: jars
	$(COMPOSE) --profile cluster up -d $(CLUSTER_SERVICES)
	@echo "三节点集群已启动。"

## e2e-test — 本地默认回归（排除 @failover 和 @slow，宿主机 JVM 执行）
e2e-test:
	cd coord-e2e-tests && mvn -T1C test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.filter.tags="not @failover and not @slow"

## e2e-smoke — 最快冒烟（仅 @smoke 场景，1-3 分钟）
e2e-smoke:
	cd coord-e2e-tests && mvn -T1C test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.filter.tags="@smoke"

## e2e-core — 常规功能回归（不含 failover/slow）
e2e-core:
	cd coord-e2e-tests && mvn -T1C test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.filter.tags="not @failover and not @slow"

## e2e-failover — 故障注入场景（需要 3 节点：先 make e2e-up-cluster）
e2e-failover:
	cd coord-e2e-tests && mvn -T1C test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.filter.tags="@failover"

## e2e-feature — 跑单个 feature (用法: make e2e-feature FEATURE=04_lock)
e2e-feature:
	cd coord-e2e-tests && mvn test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.features=src/test/resources/features/$(FEATURE).feature

## e2e-full — 完整链路（CI 或发布前自检，容器化 runner）
e2e-full: build
	$(COMPOSE_CI) --profile cluster --profile test up -d --build $(CLUSTER_SERVICES)
	$(COMPOSE_CI) --profile cluster --profile test run --rm e2e-tests
	@echo "Full E2E suite finished."

## e2e-reset — 重置数据/安全态（停所有节点含 cluster profile，清理 volume）
e2e-reset:
	$(COMPOSE) --profile cluster --profile test down -v
	rm -f coord-e2e-tests/.cache/e2e-security.json
	@echo "Volumes removed, security cache cleared. Run 'make e2e-up' to restart."

## e2e-down — 停环境
e2e-down:
	$(COMPOSE) --profile cluster --profile test down

# ══════════════════════════════════════════════════════════════════════════════
# 兼容旧命令（alias → 新命令）
# ══════════════════════════════════════════════════════════════════════════════

## build — Build SDK base image first, then all service images
build: sdk-base
	$(COMPOSE_CI) build

## sdk-base — Build coord-sdk-base:local (SDK + Maven repo, shared by all Java builds)
## sdk-base — Build coord-sdk-base:local (pulls coord-sdk-java from Nexus)
# NEXUS_PASSWORD env var must be set (e.g. export NEXUS_PASSWORD=xxx)
sdk-base:
	docker build -f sdk-base/Dockerfile \
	  --build-arg NEXUS_PASSWORD="$(NEXUS_PASSWORD)" \
	  -t coord-sdk-base:local ..

## jars — 本地编译三个业务服务 fat-jar（依赖宿主机 JDK 17 + Maven）
jars:
	mvn -f mock-services/pom.xml -am package -DskipTests -T1C

## up — (兼容) 同 e2e-up-cluster
up: jars
	$(COMPOSE) --profile cluster up -d $(CLUSTER_SERVICES)
	@echo "All services ready."

## up-dev — (兼容) 同 e2e-up
up-dev: e2e-up

## up-ci — 显式关闭 override，使用完整镜像构建（CI 使用）
up-ci: build
	$(COMPOSE_CI) --profile cluster up -d --build $(CLUSTER_SERVICES)
	@echo "All services started in CI mode (full image build)."

## tls-certs — 生成自签 CA + per-node 服务端证书 + 客户端证书到 e2e/tls/
tls-certs:
	./scripts/gen-tls-ca.sh

## up-tls — 启用 TLS / mTLS 叠加层启动集群（需先执行 make tls-certs）
up-tls: tls-certs
	$(COMPOSE_TLS) up -d --build coord-1 coord-2 coord-3
	@echo "Coord cluster started with TLS + mTLS enabled (ca/cert in e2e/tls/)."

## test — (兼容) 容器化执行全量测试
test:
	$(COMPOSE_CI) --profile cluster --profile test run --rm e2e-tests

## test-local — (兼容) 同 e2e-test
test-local: e2e-test

## test-fast — (兼容) 同 e2e-test
test-fast: e2e-test

## test-single — (兼容) 同 e2e-feature
test-single:
	cd coord-e2e-tests && mvn test $(LOCAL_TEST_PROPS) \
	  -Dcucumber.features=src/test/resources/features/$(FEATURE).feature

## reload-% — 重新打单个服务 jar 并重启容器（无需 down/up，约 15s）
reload-%:
	mvn -f mock-services/pom.xml -pl $* -am package -DskipTests -T1C
	$(COMPOSE) restart $*

## down — (兼容) 同 e2e-down
down:
	$(COMPOSE) --profile cluster --profile test down

## clean — Stop containers, remove volumes and images
clean:
	$(COMPOSE) --profile cluster --profile test down -v --rmi local
	rm -f coord-e2e-tests/.cache/e2e-security.json

## logs — Follow logs for all services
logs:
	$(COMPOSE) logs -f coord-1 order-service pay-service inventory-service

## logs-cluster — Follow Coord cluster logs
logs-cluster:
	$(COMPOSE) logs -f coord-1 coord-2 coord-3

## ps — Show container status
ps:
	$(COMPOSE) ps

# ══════════════════════════════════════════════════════════════════════════════
# help
# ══════════════════════════════════════════════════════════════════════════════

## help — 显示所有可用命令
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║  Coord E2E — 本地开发命令                                   ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  make e2e-up            本地启动（单节点 + mock services）   ║"
	@echo "║  make e2e-up-cluster    启动 3 节点集群（failover 需要）     ║"
	@echo "║  make e2e-test          默认回归（排除 failover/slow）       ║"
	@echo "║  make e2e-smoke         最快冒烟（@smoke 场景）              ║"
	@echo "║  make e2e-core          核心功能回归                         ║"
	@echo "║  make e2e-failover      故障注入场景                         ║"
	@echo "║  make e2e-feature F=xx  跑单个 feature                      ║"
	@echo "║  make e2e-full          完整链路（CI / 发布前）              ║"
	@echo "║  make e2e-reset         重置数据 + 安全态                    ║"
	@echo "║  make e2e-down          停环境                               ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  CI / 重型命令                                               ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  make up-ci             完整镜像构建启动                     ║"
	@echo "║  make up-tls            TLS/mTLS 模式启动                   ║"
	@echo "║  make test              容器化执行全量测试                   ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  热重载 / 工具                                               ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  make reload-<svc>      重编译并重启单个 mock service        ║"
	@echo "║  make jars              本地编译所有 mock service jar        ║"
	@echo "║  make logs              跟踪所有服务日志                     ║"
	@echo "║  make logs-cluster      跟踪集群节点日志                     ║"
	@echo "║  make ps                容器状态                             ║"
	@echo "║  make clean             停容器 + 删 volume + 删镜像          ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""
