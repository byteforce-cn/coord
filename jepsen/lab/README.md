# coord Jepsen lab (in-repo) — `coord/jepsen/lab/`

宿主机侧 Jepsen lab,版本化在 **coord 仓库内**(`coord/jepsen/lab/`),与随仓
版本化的测试工程 `coord/jepsen/`(上两级目录)配套。**clone coord 即可自建 lab 并
跑通 coord 的 Jepsen 测试**——不再需要宿主机上额外摆放 `jepsen-custom/`、
`jepsen` 库 clone 等外部资产。

> 旧位置 `jepsen-custom/`(宿主机 /data 私有目录,不入库)已于 2026-09-05 起被本目录
> 取代;正在运行的旧 lab(192.168.56 / jepsen-*)不受影响,可与本 lab 并行。

## 布局与自包含性

```
coord/
└── jepsen/
    ├── project.clj / src/ / scripts/   # 测试工程(canonical,随 coord 版本化)
    └── lab/                            # 本目录 = 宿主机侧 lab(方案 D)
        ├── Makefile                    # 统一入口(双 provider 分发)
        ├── Vagrantfile                 # vagrant provider(control + n1..nN)
        ├── provision/                  # common.sh / control.sh / node.sh
        ├── files/lein-profiles.clj     # 国内镜像(可选;海外直连无需)
        ├── keys/                       # 由 make up 自动生成,git 忽略,不入仓
        ├── examples/noop-test/         # 框架冒烟测试(无需被测库)
        └── docker/docker-compose.lab.yml  # docker provider(compose)
```

* Makefile / Vagrantfile 的路径均以**本文件位置**为锚(`REPO_ROOT`/`TEST_DIR`
  自动推导),与 coord 或 Jepsen 库 clone 在宿主机的具体位置无关。
* **Jepsen 库本体不再依赖宿主机 clone**:vagrant provider 下 `provision/control.sh`
  会在控制机内自行 `git clone jepsen-io/jepsen`(可用 `JEPSEN_LIB_REPO` /
  `JEPSEN_LIB_REF` pin 版本),`make setup` 对其 `lein install`;docker provider
  下 `jepsen-control` 镜像已内置 lein-install 好的 `jepsen 0.3.14-SNAPSHOT`
  (与 `coord/jepsen/project.clj` 依赖一致)。
* coord release 二进制(~28MB)**不入库**:`make upload` 从当前 checkout
  `cargo build --release -p coord` 并部署/挂载。
* 双 provider 在控制机共享同一套布局(`/root/coord-test`、`/root/nodes`、
  `/root/.ssh/id_ed25519`),因此 `coord-soak.sh`、`db.clj` 的默认路径零改动。

## 前置条件

| Provider | 需要                                    |
|----------|----------------------------------------|
| vagrant  | Vagrant >= 2.3 + VirtualBox 或 libvirt/KVM |
| docker   | Docker + Compose v2(免虚拟化)             |

网络:默认按国内环境走镜像(`JEPSEN_APT_MIRROR`/`JEPSEN_GH_PROXY` 等可覆盖);
海外直连环境可自行调整,或直接用 docker provider 的官方镜像。

## 快速开始(vagrant,推荐用于全矩阵与 72h soak)

```bash
cd jepsen/lab
make up              # 首次:自动生成 keys/ + 开机并 provision(控制机内克隆 Jepsen 库)
make setup           # 一次性:对控制机内的 Jepsen 库 lein install
make upload          # 部署 jepsen/ + 构建 coord 二进制 -> 控制机 /root/coord-test
make quick           # 20s sanity run (no nemesis)
make test            # WORKLOAD/NEMESIS/TIME_LIMIT/... 可覆盖
make test NEMESIS=kill TIME_LIMIT=120
make matrix          # 全矩阵(workloads x nemeses),失败即停
make soak            # 72h soak(控制机后台);soak-status/soak-tail/soak-stop/soak-results
```

> 与旧 lab 隔离:本 Vagrantfile 默认子网 `192.168.57`、VM 名前缀 `coord-jepsen-`,
> 与旧 `jepsen-custom/`(192.168.56 / `jepsen-*`)并行不冲突。可调:
> `JEPSEN_SUBNET`、`JEPSEN_VM_PREFIX`、`JEPSEN_NODES`、`JEPSEN_MEM` 等(见
> Vagrantfile 头部注释)。

## 快速开始(docker,日常开发/冒烟/CI)

```bash
cd jepsen/lab
make docker-up        # docker compose up -d(拉取官方 jepsen-control/node 镜像)
make docker-upload    # 构建二进制 + 在控制容器内准备 VM 一致布局(bind-mount 仓库)
make docker-quick     # 20s sanity
make docker-test      # coord test
make docker-matrix    # 全矩阵
```

* 镜像默认来自 `ghcr.io/nurturenature/jepsen-docker/`;国内网络拉取不畅时可设
  `JEPSEN_REGISTRY=` 并在本机 `jepsen-docker/` 目录 `./bin/docker-build.sh`
  本地构建后复用。
* `coord` 仓库根目录 bind-mount 到控制容器 `/opt/coord`(rw):改 `coord/jepsen/`
  源码即时生效,结果写回宿主机 `coord/jepsen/store|soak`(均已 git-ignore)。
* 容器是比 VM 弱的故障注入/长跑宿主:**72h soak 与全矩阵建议仍用 vagrant provider**;
  docker 用于日常迭代、冒烟与 CI。

## 两种 provider 都支持的常见操作

任意 `make <target>` 默认走 vagrant;加 `JEPSEN_PROVIDER=docker` 或直接
`make docker-<target>` 走 docker:

| 目标 | 说明 |
|------|------|
| `up` / `halt` / `destroy` / `status` | lab 生命周期(vagrant 或 compose) |
| `setup` | 控制机内 Jepsen 库 lein install(vagrant;docker 镜像已内置,no-op) |
| `upload` | 部署 jepsen/ + coord 二进制(vagrant 上传;docker 走 bind+准备布局) |
| `smoke` / `quick` / `test` / `matrix` | noop 冒烟 / 20s / 单测 / 全矩阵 |
| `soak*` / `logs` / `soak-diag` | 72h soak 与诊断 |
| `ping` / `info` | 连通性 / 当前 provider 与工具状态 |

所有测试以 root 在控制机运行;结果落在 `/root/coord-test/store/coord/<date>/`。

## 变更与维护

* 测试源码与脚本的 canonical 位置是 `coord/jepsen/`(仓库外请勿另存副本);
  lab 外壳的 canonical 位置是 `coord/jepsen/lab/`。
* 新增/修改 `provision/*.sh` 后:对已存在 VM 执行 `make provision`(单机
  `make provision NAME=n1`)即可生效,幂等。
* soak 运行期间 `make upload` 会被拒绝(保护正在跑的验证)。
* 本目录 `keys/`、`.vagrant/` 不入仓;Vagrantfile 与 `make up` 会在首次自动生成
  lab 密钥对(对外共享前请按需重新生成)。
