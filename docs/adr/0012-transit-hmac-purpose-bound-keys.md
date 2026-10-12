# ADR-0012: Transit HMAC 用途化密钥与轮换窗口（G-TR-3）

- 状态：accepted
- 日期：2026-10-12
- 决策者：维护团队（需求来源：EIS 仓 `eis/docs/COORD-REQUIREMENTS.md` G-TR-3 / `write-only-fields.rules.yaml` DRV-002、DRV-005；跟踪 issue #54）

## 背景

EIS 的「写入专用派生字段」（如 hr 的 `id_number_hash` 防重散列）需要一个稳定、可轮换的
HMAC 生产面，其规则表（DRV-002 / DRV-005）要求：**密钥按用途隔离**；**新写入恒用主密钥**；
**防重比对覆盖轮换窗口内全部接受密钥**（按同一原始值复算密钥环所有版本的散列变体）。
coord Transit 若要作为该能力的可选平台映射，必须满足这两条，否则消费方只能自持注入密钥。

现状（实现锚点 `coord-agent/src/services/transit.rs`）：

- `TransitService` 只有一把 `hmac_key`（由主材料 HKDF 派生，info = `coord-transit-hmac-v1:` || kek_id）；
  历史材料循环明确「仅派生解密用 KEK（不参与加密/HMAC）」；
- `TransitHmacSignRequest` 只有 `data` / `algorithm`，签名不含用途与材料标识；
  `HmacVerify` 只能按主材料复算；
- 后果：KEK 轮换 ⇒ 存量 HMAC 全部不可复算；`Rewrap` 只迁移 DEK（G-TR-1），HMAC 无迁移路径。

参照 **Vault Transit** 的既有解法（`builtin/logical/transit/path_hmac.go`、`path_keys.go`、
`path_rotate.go`）：

| Vault 概念 | 本 ADR 的对应 |
|:--|:--|
| named key（`hmac/<name>`） | `purpose`（用途标签） |
| key version（`key_version` / 签名前缀 `vault:v<N>:`） | 材料标识 `key_id` |
| 签名携带版本，验签**必须**按签名中的版本复算 | 签名响应携带 `key_id`，验签**必须**显式给出 `key_id` |
| `key_version` 可显式指定（复算/变体） | `HmacSign.key_id` 可显式指定 |
| 轮换后新写用最新版本、旧版本仍可验 | 新签名用主材料、历史材料在注入期可验 |
| `min_decryption_version` 越界 → 显式错误 | 材料未注入 → `FAILED_PRECONDITION`（fail-loud） |

**不**照搬的部分：Vault 每个 named key 独立材料、自动轮换与版本归档、`min_encryption_version`
策略门；coord Transit 仍是「单一注入材料 + 用途派生」，无自动轮换（边界见 B-SE-2 / B-SE-6）。

## 决定

### 1. 用途化派生（Vault named key 类比）

- `purpose` 为**必填**的不透明稳定标签：非空、不含 NUL、≤128 字节；为空或非法 ⇒
  `INVALID_ARGUMENT`（「未声明用途 fail-closed」的落点）。用途版本化由调用方在标签内表达
  （如 `eis:hr:employee:id_number:v1`），服务端不解析。
- 派生（HKDF-SHA256，**单次** Extract+Expand，段长前缀编码消除歧义）：

  ```text
  info     = "coord-transit-hmac-v1:" || u32be(len(kek_id)) || kek_id
                                        || u32be(len(purpose)) || purpose
  hmac_key = HKDF-SHA256(材料, salt=None, info)
  ```

  该编码是规范形式（不存在「以根为 PRK 再 Expand」的第二读法）；不同 `purpose` 派生的密钥不同
  ⇒ 互不可验（负控制）；同一材料标识在任一 agent 派生结果一致。
- **历史材料同样参与 HMAC 派生**（作废「历史材料不参与加密/HMAC」注释）。历史材料仅按
  `key_id` 显式引用时使用；主材料缺省参与签名。
- 不设用途白名单：用途集合属于域语义（消费方自定），白名单会把域配置塞进 agent；
  调用方已由 `coord:transit:crypto` 能力门控制（与既有 `Encrypt`/`Decrypt` 同一信任域）。

### 2. `key_id`（Vault key version 类比）

`key_id` = 注入材料标识（主材料 = 配置 `kek_id`；历史材料 = `COORD_TRANSIT_KEK_OLD` 条目标识）。

| 消息 | 新增字段 | 语义 |
|:--|:--|:--|
| `TransitHmacSignRequest` | `string purpose = 3` | 必填；空/非法 ⇒ `INVALID_ARGUMENT` |
| | `string key_id = 4` | 可选；缺省/空 = 主材料（**新写入口径**）；显式给出 = 指定材料（复算散列变体 / 迁移核对） |
| `TransitHmacSignResponse` | `string key_id = 3` | 服务端分配：实际参与签名的材料标识；**调用方必须与签名一起持久化** |
| `TransitHmacVerifyRequest` | `string purpose = 4` | 必填；同上 |
| | `string key_id = 5` | **必填**；只按该材料复算（Vault「签名携带版本」的等价物） |

- 给定 `key_id` 未注入（`HmacSign` 显式给出 / `HmacVerify` 给出）⇒ `FAILED_PRECONDITION`
  （与 `Rewrap` 的「材料未注入」同口径），**不是**静默 `valid=false`；签名长度不符 /
  不匹配 ⇒ `valid=false`（不是错误）。
- 验签**必须**给出 `key_id`：无 fallback 试解。这保证「材料下线 ⇒ 明确错误」对任何已持久化
  `key_id` 的签名无条件成立（验收口径 b）；本改动前产生的无 `key_id` 签名不可验（coord 的
  HMAC 生产面尚未有生产消费者，EIS 尚未切换；如需保留只能重签）。
- 复算确定性：同一 `(purpose, data, key_id, algorithm)` 在任一注入相同材料的 agent
  结果一致（验收口径 c；跨 agent 一致性要求见 §4）。

### 3. 轮换窗口与旧材料下线

- **新签名用主材料**（调用方不传 `key_id`）；旧签名携带 `key_id`，在历史材料注入期可验；
  材料从注入集合移除后，按该 `key_id` 复算 ⇒ `FAILED_PRECONDITION`（明确错误 + 可用材料列表）。
- **消费方义务**：`HmacSign` 的 `key_id` 必须与签名一起持久化（EIS 的 DRV-005 密钥环语义
  本身要求按密钥版本组织存量散列，故不引入额外模型）；未持久化 `key_id` 的签名在材料下线后
  无法复算，也不可 fail-loud——这是调用方数据模型缺陷，不是服务端可补偿项。
- **HMAC 不参与 `Rewrap`**（`Rewrap` 仍只迁移 DEK）：HMAC 的依赖是「在库签名对历史材料的
  引用」，迁移等价物 = 消费方按新主材料重新散列并回写（DRV-005 的重建流程），coord 不代做。
- 回退路径：重新注入历史材料（同一 `id` / 同一材料）即恢复复算；窗口内任何失败都可回退到
  「新旧并存」。
- 文档落点：`docs/production/ops/runbook.md` §4.4（增补 HMAC 轮换 / 下线 / 回退）、
  `docs/production/consumer-contract.md` §10、`docs/production/ops/boundaries.md` B-SE-6。

### 4. 跨-agent 材料一致性（写入契约）

- 同一集群内所有 agent 必须注入**相同材料 + 相同材料标识**（主 + 历史）；这是既有
  B-SE-2/B-SE-6 要求的延伸，本 ADR 将其显式写入 `consumer-contract.md` §10 与 proto 头注。
- 派生无 per-agent 随机量：一致性由「材料一致」唯一决定；缺历史材料的 agent 在对应
  `key_id` 上 fail-loud（不静默返回 false 或错误结果）。
- **接受密钥集合的同步义务**：消费方「接受密钥环」必须与 agent 注入的历史材料集合一致
  （同一 runbook 步骤维护）。coord 不提供接受集合的下发/枚举 API（非目标）：集合缺失会
  使防重窗口静默欠覆盖（消费方侧可检测：存量散列中存在未知 key_id），集合多余会在签名时
  `FAILED_PRECONDITION` 显式暴露。
- 破坏一致性的形态（应避免/自担）：不同 agent 同 id 不同材料、同材料不同 id、只更新部分
  agent 的历史材料集合。

### 5. 契约与错误码（Minor：`contracts/v1.5.0`）

- 新增字段在 wire 层向后兼容（旧客户端可省略编号）；服务端行为收紧见下，
  不改既有字段编号/类型；`buf breaking` 基线不变。
- **行为收紧（consumer notice）**：`HmacSign/HmacVerify` 的 `purpose` 与 `HmacVerify` 的
  `key_id` 由缺省变为必填；未提供（含旧 SDK 0.2.1 二进制）⇒ `INVALID_ARGUMENT`；
  `HmacSign` 响应新增 `key_id`，调用方必须持久化。不支持算法与用途非法统一
  `INVALID_ARGUMENT`（原为 `INTERNAL`；无文档承诺，无已知消费者依赖）。
- Java SDK：**移除**无用途/无 `key_id` 的旧 HMAC 方法（`hmacSign` / `hmacVerify`），
  由用途化 API 取代：`hmacSignWithPurpose`（支持显式 `key_id`，返回携带 `key_id` 的
  结果类型）与 `hmacVerifyWithPurpose`（`purpose` + `key_id`）。旧方法既无法携带
  `key_id`（sign 返回 `byte[]` 丢身份）、也无法满足验签必填 `key_id`，保留只会留下
  不可验证的半截 API；SDK 0.3.x 尚未发布（Central 最新为 0.2.1），且旧调用对收紧后的
  agent 本就不可用。
- 契约记录：`apis/contracts/CHANGELOG.md` 新增 `[contracts/v1.5.0]`、`STATUS.md` transit 行
  补 G-TR-3、`WHITEPAPER.md` 相关口径行同步。
- 版本分类：判为 **Minor**——wire 层仅新增字段；行为收紧面的**已知消费者为 0**，且已在
  `apis/contracts/CHANGELOG.md`（`contracts/v1.5.0`）按 §11.2 模板公告。不走 Major 的
  `coord.transit.v2` + 长期双跑（无已知消费者，代价不成比例；规则见 WHITEPAPER §4）。

## 后果

- 收益：EIS 可用 transit 托管域密钥并把 DRV-002/DRV-005 映射到平台（purpose 隔离 +
  轮换窗口 + 变体复算）；HMAC 与 KEK 同一材料生命周期，轮换不再使存量散列失效；
  失败显式（无静默降级）。
- 代价：`purpose` 与验签 `key_id` 成为必填，HMAC 调用方全部需要改造（消费方告知 +
  SDK 更新），并需按約定持久化 `key_id`；历史材料集合同时承担 DEK 解密与 HMAC 复算两份责任，
  下线前需两侧依赖都确认；每次签名/验签做 HKDF（可忽略成本）。
- 不变量：单一注入材料模型不变（B-SE-2）；`Rewrap` 语义不变（仅 DEK）；无外部 KMS/HSM；
  默认关闭与 fail-closed 姿态不变（ADR-0001）。
- 非目标：批量签名 / 变体批量返回；接受密钥集合的枚举/下发 API；每用途独立材料托管；
  自动轮换与版本归档/删除策略；`min_encryption_version` 类策略门；非对称签名（B-TR-1 不变）。

## 参考

- 需求与规则（外部工件，单一归属在 EIS 仓库）：`eis/docs/COORD-REQUIREMENTS.md`（G-TR-3）、
  `eis/01-data-contract/write-only-fields.rules.yaml`（DRV-002 / DRV-005）、
  `eis/00-freeze/adr/0005-write-only-derived-fields.md`。
- 参照实现：Vault Transit `builtin/logical/transit/path_hmac.go`（`key_version` 签名、
  按签名版本验签）、`path_keys.go`（`min_decryption_version` / `min_encryption_version`）、
  `path_rotate.go`（轮换后新写用新版本、旧版本可解）。
- 契约：`apis/contracts/proto/coord/transit/v1/transit.proto`、
  `coord-proto/src/proto/transit.proto`、`apis/contracts/STATUS.md`、
  `apis/contracts/CHANGELOG.md`（`contracts/v1.5.0`）。
- 实现与边界：`coord-agent/src/services/transit.rs`（`TransitKekKeyring` / `hmac_sign` /
  `hmac_verify`）、`coord-agent/src/services/grpc_handlers.rs`（错误码映射）、
  `docs/production/consumer-contract.md` §10、`docs/production/ops/runbook.md` §4.4、
  `docs/production/ops/boundaries.md` B-SE-6；Java SDK `TransitClient` / `TransitClientImpl`。
- 相关 ADR：ADR-0001（默认关闭）、ADR-0009（dev 预设 KEK）、ADR-0010（G-TR-1 多材料）。
