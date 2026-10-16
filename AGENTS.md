# AGENTS.md — 给 AI 助手的仓库规范

编辑本仓代码 / 文档时，请遵守以下约定（与 `CONTRIBUTING.md` 一致）：

## 构建与测试

- Rust：`cargo build` / `cargo test`；`cargo clippy --all-targets --all-features`；`cargo fmt --all -- --check`。
- Java（`coord-java-sdk` / `java-example`）与 UI（`coord-ui`）：见 `CONTRIBUTING.md`。

## 注释（.rs / .toml / .yml 等）

- **只写当前「为什么」**：不变量、陷阱、契约。
- **不写**：日期（真实期限/测试数据除外）、会漂移的章节号引用（如「第 3.4 节」）、一次性任务编号、
  「此前 / 修复前 / 旧版为 …」的变更史叙事。
- 跨模块决策写进 `docs/adr/`（单一定义），注释里引用「（见 ADR-00xx）」。
- 运行错误消息中的稳定诊断锚点（如 `R-SEC-04`）**逐字保留**——它们与 runbook / 测试断言联动。
- 不新增 `TODO` / `FIXME`。

## 文档（.md）

- 文档描述当前态（建议头注 Owner / Last verified）；现状文档放 `docs/production/`。
- 设计决策放 `docs/adr/`；同一事实只维护一处（单一归属），不复制粘贴其他文档内容。
- 新增/修改引用后跑 `bash scripts/check-doc-refs.sh`（悬空路径必须为 0）。

## 提交与检查

- 断言消息不得包含一次性任务编号；检查缺失必须 fail-closed（拒绝把「未跑」伪装成「通过」）。
- **提 PR（含对已开 PR 分支的推送）前，必须在本机跑通 CI 基础门禁**——禁止把「等 CI 跑」
  当作首次验证。与 `.github/workflows/ci.yml` 的 lint / security-audit job 对齐：
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --lib --bins -- -D warnings`
  - `bash scripts/check-panics.sh`
  - `cargo audit` 与 `cargo deny check`（advisories / licenses / bans / sources）

## 发布（Release）

发版动作 = 升版提交 → 推 tag → 发布链路自动出产物；发布对象一律由链路创建，agent 只做只读验证。

- **升版**：推 tag 前提交 `chore(release): vX.Y.Z`——workspace `Cargo.toml`（version 与内部 path
  依赖）、`Cargo.lock`、`coord-java-sdk/pom.xml` 及对外版本行（`grep -rn` 旧版本号确认无残留，覆盖
  `README*.md`、`SECURITY.md`、`deploy/`、`docs/production/`）。tag 名与 workspace 版本必须同名——
  tag 触发的 smoke 会断言 `coord --version`。
- **二进制 / 镜像**：`git tag vX.Y.Z && git push origin vX.Y.Z` 即完成。`.github/workflows/release.yml`
  由 tag 触发：构建 `coord` 二进制 + SHA256、创建同名 GitHub Release、推
  `ghcr.io/byteforce-cn/coord` 镜像；验证用只读命令（`gh run watch`、`gh release view`）。手工
  `gh release create` / `gh api` 会与 workflow 上传的同名 asset 撞车（`artifact already exists`）
  并让发布失败。只验构建的干跑用 `workflow_dispatch`（不建 release、不推镜像）。
- **coord-java-sdk（Maven Central）**：凭证在本机固定路径，只读引用、勿入库：
  - `/root/.maven-gpg-keyfpr` → `-Dgpg.keyname`；
  - `/root/.maven-gpg-passphrase` → `MAVEN_GPG_PASSPHRASE`；
  - `/root/.maventoken` → `~/.m2/settings.xml` 中 server `central` 的 Portal 用户 token
    （与 pom `publishingServerId` 同名）。

  ```bash
  cd coord-java-sdk
  JAVA_HOME=/opt/jdk-21.0.12.1+1 MAVEN_GPG_PASSPHRASE="$(cat /root/.maven-gpg-passphrase)" \
    /opt/apache-maven-3.9.16/bin/mvn -Prelease clean deploy \
    -Dgpg.keyname="$(cat /root/.maven-gpg-keyfpr)"
  ```

  须用 Maven 3.9.x（PATH 上的 `mvn` 是 3.10，staging 元数据残留会被 Central 拒收；其余前置条件见
  `coord-java-sdk/pom.xml` 的 `release` profile 注释）。deploy 后为 validated（`autoPublish=false`），
  在 central.sonatype.com 人工确认发布；生效后再同步文档中的「最新发布」版本行。

## 临时文件（强制）

- **所有临时文件、草稿脚本、日志、下载产物一律放系统 `/tmp` 下**（建议 `/tmp/coord-*` 前缀，
  如 `/tmp/coord-pr/`、`/tmp/coord-tmp/`）。
- **禁止**在仓库工作区内创建 `tmp/`、`scratch/` 等临时目录或在其中写文件——会污染源码树，
  干扰工作区视图与管理（即使已被 .gitignore 忽略也不允许）。
- 仓库 `tmp/` 下**既有**的历史脚本/产物保持原位，**不要迁移、删除或改名**（避免与既有流程/会话冲突）；
- 构建产物按各自约定入 `target/`（已 ignore）；其余大文件、备份、探测结果一律放 `/tmp`。

