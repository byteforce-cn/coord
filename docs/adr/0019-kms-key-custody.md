# ADR-0019: KMS 化——coord 托管密钥材料与远程签名

- 状态：accepted
- 日期：2026-11-20
- 决策者：维护团队
- 取代：`boundaries.md` **B-TR-1**（不提供远程非对称签名）与 **B-PKI-1** 中「密钥交付持有方」的
  定位；`ADR-0010 §16`（G-TR-2）标注 superseded，原文保留为历史决策记录

## 背景

coord 的 PKI 与 transit 按 `ADR-0010 §16`（G-TR-2）定的是「对称加密 + HMAC，PKI 只签发不托管」：
`IssueCert` 返回 `key_pem`，`GetCertByCN` 返回私钥并按「私钥读取权」授权，`VerifyCert` 只作为
存在性/有效期检查。

上游消费方（seedj）的契约同时要求两条 coord 现在给不了的东西：

- **PKI**：`signCsr`、`revoke` + CRL、按 serial 只读查询、`CertificateUsage` 精确 KU/EKU；
- **Transit**：命名密钥 + 版本三边界、非对称 `sign`/`verify`、`generateDataKey`、导入导出。

其中**非对称签名 + 私钥不出域**正是 B-TR-1 写明的重议触发条件：

> 组织 KMS 方向确定要求「私钥不出域」时另行立项评估（候选：transit 增 ES256 Sign，或独立 KMS 服务；
> 先设计后实施）

触发条件成立，且组织方向已定为「coord 本身成为密钥托管面」。本 ADR 记录该决定与实施口径。

## 决定

### D1 定位：coord 提供 KMS 面

coord 托管密钥材料并代理密码学操作：私钥默认保存在 coord 内、**默认不可导出**，调用方通过
`sign` / `verify` / `encrypt` / `decrypt` / `hmac` / `generateDataKey` 使用，拿不到原始材料。
PKI 与 transit 共用同一套密钥档案与签名原语——**不做两套密钥存储**。

### D2 密钥档案是单一对象

```
/_transit/keys/{name}                   档案：type / latest_version / min_decryption_version
                                              / min_encryption_version / deletion_allowed
                                              / exportable / created_at / versions[]
/_transit/keymat/{name}/{version}       版本材料（用 KEK 包裹的私钥或对称密钥）
```

材料键使用独立前缀（而不是 `.../v/{version}` 后缀）：密钥名允许 `/`（PKI 的
`pki/leaf/{cn}` 与 `pki/ca`），后缀解析会让「名字里恰好含 `/v/`」与材料键混淆。

三个版本边界都在服务端，请求可显式指定版本；`version = 0` 表示 `latest_version`。
PKI 的 CA key 固定在 `pki/ca`（never_exportable），leaf key 走同一档案
（`pki/leaf/{cn}`），证书的轮换代次即密钥版本。两条命名前缀都归 PKI 服务独占，
Transit 的公开写面对 `pki/` 一律 `PERMISSION_DENIED`。

### D3 材料保护级别不变：软件 KEK，非 HSM

版本材料由现有 `transit` KEK 包裹（`B-SE-2` 的注入模型不变）。**KMS 化不提升保护级别**：
主机被控 ⇒ KEK 与被包裹材料同时被控。`security.md` §3.3 的「非 HSM」边界继续有效，
触发 HSM/PKCS#11 的条件（合规要求硬件保护、跨组织共享信任根、泄露复盘要求不可导出）另立 ADR。

### D4 非对称签名原语

支持 `ED25519` / `ECDSA_P256|P384|P521` / `RSA_2048|3072|4096`（PKCS#1 v1.5 与 PSS）。

- `prehashed = true` 时 `hash_algorithm` **必填**，缺省即 `INVALID_ARGUMENT`——不设静默默认，
  静默默认会让同一签名在两种口径下「验得过/验不过」。
- `ED25519` 不接受 `prehashed = true`（EdDSA 无预哈希路径），显式拒绝而非忽略。
- 签名响应**必须回显 `key_version`**，验签方据此定位版本（沿用 `ADR-0012` 关于「验签必填材料标识」的教训）。
- 材料未注入/版本不存在 ⇒ `FAILED_PRECONDITION`，不是 `valid = false`（同 `ADR-0012` fail-loud 口径）。

### D5 密文格式版本化，且**不兼容**旧格式

密文自描述，携带 `key_name` 与 `version`：

```
CCT3 || key_name_len(u8) || key_name || version(u32) || nonce(12) || dek_packet(60) || ciphertext
```

现行 `CCT2`（无 key name、无版本）**直接移除**，不保留只读兼容路径——双路径是长期负债。
同理 `Rewrap(dek_id)`（服务端 dek 档案语义）替换为「密文进、密文出」的 rewrap，不保留旧 RPC 语义。

### D6 PKI 私钥留库

- `IssueCert` / `RotateCert` / `RenewCert` **不再返回 `key_pem`**，返回
  `key_material = { kind: coord_managed, key_name, key_version }`；
- `GetCertByCN` 的私钥读取路径**删除**。私钥唯一出口是 `ExportKey`（受 D7 约束），
  避免两套私钥出口；
- CA 私钥进同一密钥档案，但**恒不可导出**，任何能力都不得导出；
- TLS 握手签名由调用方实现委托签名的 `KeyManager`，在 `engineSign` 中调 `sign`（见 D9）。

### D7 `exportable` 默认 false，导出是独立授权

导出私钥与「私钥不出域」是相反主张，因此：

- `exportable` 默认 `false`；`false` 时 `ExportKey` ⇒ `FAILED_PRECONDITION`；
- 导出需要**独立能力**（`coord:transit:key_export`），不挂在 `coord:transit:crypto` 上；
- 每次导出写审计（见 D8）。

### D8 能力拆分与审计前提

现有 `coord:transit:crypto` 是粗粒度单点，KMS 化后必须拆开：

| 面 | 能力 |
|:--|:--|
| 读元数据 | `coord:transit:key_read` / `coord:pki:cert:read` |
| 加解密 / HMAC / 签名 | `coord:transit:crypto` |
| 密钥生命周期（create/rotate/config/delete/trim/import） | `coord:transit:key_manage` |
| 导出材料 | `coord:transit:key_export` |

能力注册表的 ID 语法是 `domain:service:action` **三段**（`capability_def` 只接受
三段）；因此上面的子资源面写成 `coord:transit:key_read` / `key_manage` /
`key_export`，而不是四段式。PKI 私钥没有第二个导出 RPC（D6）：leaf/CA 都是
transit 档案里的密钥，导出走同一个 `Transit/ExportKey` 与同一个
`coord:transit:key_export` 能力；`pki:cert:read` 只覆盖证书/元数据。

**审计前提**：`coord-server/src/audit/mod.rs` 当前只覆盖鉴权拒绝与认证路径，管理操作的调用方
身份传递是 v2 backlog。KMS 的写操作（create/rotate/delete/import/export/trim）必须带
`actor` + `resource = key_name` 进审计；在审计补齐之前，KMS 写操作面**不得**对外宣称「已审计」。

### D9 TLS 委托签名的可用性口径

允许「握手签名回调远程 KMS」（这是消费方 `sslConfig` + 委托 `KeyManager` 的前提），但：

- 每次握手 1~2 次远程签名是**显式成本**，必须给延迟预算并以会话复用（session resumption）
  消化；不得把「每连接两次往返」当默认稳态；
- 若实测达不到可接受延迟，回退方案是「进程内持有私钥 + coord 只做签发」，此时 `CoordManaged`
  分支从契约中删除，而不是保留一个永远抛异常的分支。

### D10 明确不做

- **`hash` / `random` 下放**：本地 `sha2` / `OsRng` 零成本，经 RPC 只是把可用性绑到集群。
  仅当出现「随机数必须可审计」的明确需求再评估。
- **`cache-config` / 客户端离线缓存**（Vault Enterprise 语义）：需要把包装后的材料下发到客户端
  并维护拉取/失效协议，与「私钥不出域」目标冲突。
- **`convergent` 加密**：确定性密文泄漏「明文相等」这一事实，且与随机 nonce 包格式不兼容。

### D11 无兼容包袱

当前系统没有需要保活的历史数据与调用方，因此本 ADR 的所有改动**不做兼容层**：
直接换形状、直接删旧路径。这条约束**降低实现成本**，不改变能力现状——在实现交付前，
`boundaries.md` 仍按现状描述（B-TR-1 / B-PKI-1 原文继续成立）。

## 后果

- **安全**：私钥不再随签发响应扩散，`GetCertByCN` 的「私钥读取权」授权面消失；但材料保护级别
  仍是软件 KEK（D3），且 `exportable` / CA key 的边界必须靠能力授权守住。
- **可用性**：签名单点化。KMS 不可用 ⇒ 依赖它的 TLS 握手失败。需在部署文档写明这是新的
  硬依赖，并给出「本地材料 vs 远程签名」的选择口径。
- **运维**：密钥档案成为必须备份的对象；KEK 丢失 ⇒ 全部材料不可解（比现在丢一个 CA 更严重）。
  备份/恢复流程要进 runbook。
- **契约**：`coord.transit.v1` 与 `coord.pki.v1` 都进入 wire 变更（新 RPC + 字段删除），
  需要 `apis/contracts` 的版本推进与 `STATUS.md` 同步。

## 替代方案

- **独立 KMS 服务**：多一个部署单元、多一套高可用与鉴权面，而密钥与协调原语本就同处一个信任域。
  否决。
- **维持「密钥交付持有方」**：即现状。消费方已把「私钥不出域」写进契约，维持现状等于拒绝接入。
  否决。
- **只做 `signCsr`（外部密钥），不做托管**：能覆盖「私钥不出域」，但给不了 TLS 委托签名、
  也解决不了 transit 的命名密钥面。作为**并行支线**保留（见 #428），不替代本 ADR。
