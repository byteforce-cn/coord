# ADR-0020: PKI 吊销面——RevokeCert / GetCertStatus / CRL / Tidy

- 状态：accepted
- 日期：2026-11-21
- 决策者：维护团队
- 取代：`boundaries.md` **B-PKI-1** 的「无吊销」口径（②）；B-PKI-1 的「私钥留库」（①）
  与「非 HSM」（③）继续有效，见 ADR-0019 D3。

## 背景

seedj 的 `PkiProvider` 契约要求 `revoke(serial, reason)`、`status(serial) = REVOKED`、
`getCrl()` / `rotateCrl()`，并明确「已吊销身份不静默复活」。`#375` 曾裁决「吊销由 seedj
在 KV 组合（`/_seedj/pki/revoked/{serial}`），coord 不扩 RPC」，代价写在当时：

> `VerifyCert` 只验签名，**不认识任何外部吊销表** ⇒ seedj 的吊销只对 seedj 自己的校验路径
> 生效；第三方验签方（包括直接调 coord `VerifyCert` 的服务）看不到吊销。

`security.md` 的重估触发条件已命中「身份签发面升级为对外产品承诺（跨组织 / 跨安全域共享
同一 CA）」：多实例共享同一 coord CA 已在用（`#375` 的验收项即「两个 seedj 实例共享同一
coord CA」）。因此吊销必须成为 **CA 侧的权威事实**，而不是某个消费方的私有账本。

## 决定

### D1 吊销账本是单一事实源

`/_pki/v1/revoked/{cert_serial}`（JSON）是唯一权威记录：

```
{ cert_serial, record_serial, common_name, reason, revoked_at, by }
```

- 键 = **证书内嵌的 X.509 serial**（小写 hex）。CRL 条目与第三方验签方看到的是同一个值。
  历史记录的 `record_serial` 可能与内嵌值不同（见 D8），记录里保留它只为回显。
- 账本落在共享 KV（coord-server，redb + Barrier 加密），与 CA 同存储 ⇒ 跨实例、跨重启一致。
- **永久保留**：serial 的 `revoked` 是永久事实，不随证书过期、不随 Tidy 回收。
  重复吊销幂等，保留首次的 `reason` / `revoked_at` / `by`。
- `by` 是审计事实，取自鉴权主体（CCT `sub`；无身份上下文时 `anonymous`），
  **不接受调用方自报**。

### D2 状态域与读面

对外状态统一为 `active | retired | expired | revoked`：

- `revoked` 覆盖其余存储态，也覆盖 `expired`（过期且被吊销 ⇒ `revoked`）；
- `GetCertStatus(serial)` 接受记录 serial 或证书内嵌 serial；未知 ⇒ `NOT_FOUND`；
- `GetCertBySerial` / `ListCerts` 用账本覆盖状态；`GetCertByCN` 对已吊销的当前证书返回
  `NOT_FOUND`（它不是「当前有效证书」）；
- `VerifyCert` 在签名/有效期通过后查账本，命中 ⇒ `valid=false`。

### D3 不静默复活

「同 CN 的 get-or-create」不能在旧身份被吊销后悄悄换一张新证书：

- `IssueCert` 对「当前证书已吊销」的 CN ⇒ `FAILED_PRECONDITION`（点名吊销，不改签）；
- `SignCsr` 同口径 ⇒ `FAILED_PRECONDITION`（CSR 不得复活被吊销的 CN）；
- `RenewCert(serial)` 对已吊销 serial ⇒ `FAILED_PRECONDITION`（续期即复活）；
- `RotateCert` 是**显式**换新身份的路径：允许，旧 serial 仍永久 `revoked`；
- 外部密钥（CSR 签发）证书的 `RenewCert` / `RotateCert` 本来就要求重新提交 CSR
  （ADR-0019 D6），而 `SignCsr` 对已吊销 CN 也拒绝 ⇒ **外部密钥身份的吊销是终态**，
  继续使用必须换新 CN。这是显式取舍：宁可让调用方改 CN，也不给「同 CN 静默复活」留后门。

### D4 CRL

- CA 签名 CRL（rcgen；CA 证书已带 `CrlSign` 用途位），条目 = 吊销账本全量；
  entry 的 serial 取证书内嵌值，reason 用 RFC 5280 CRLReason 数值。
- CRL 编号持久化在 `/_pki/v1/crl`（`{crl_number, content_hash, this_update, next_update}`）：
  - `GetCrl` 在内容（吊销集合）未变且 `next_update` 未过时复用编号与时间窗口；
    `next_update` 已过则换号重签（没有后台发布任务可依赖）；
  - 内容变化或显式 `RotateCrl` ⇒ 分配 `number + 1` 并 CAS 持久化；
  - `content_hash` = 排序后 `cert_serial + reason` 的 SHA-256 ⇒ 「编号 ↔ 条目集」一致。
- `next_update = this_update + crl_ttl_hours`（默认 24h）。coord **不跑**定时发布任务：
  编号在读取/显式轮换时分配。
- 分发只走 RPC：**不新增 HTTP 端点**；缓存与刷新节奏由消费方负责。
  ⇒ **吊销生效延迟 = 消费方的 CRL / KV 缓存窗口**；只有调 `VerifyCert` 的路径是即时生效。

### D5 Tidy（保留期回收）

`Tidy(retention_seconds)` 回收 `not_after + retention` 之前到期的证书记录：

- 历史（retired）记录与过期 active 记录都回收（active 用版本化 CAS 删除，避免误删并发轮换
  出的新证书）；
- **不回收**吊销账本，也不回收已吊销的证书记录：active 槽位上的已吊销记录同时承担
  「同 CN 不得静默重签」的阻断职责；
- `retention_seconds = 0` 用服务端默认 `history_retention_hours`（默认 24h）。

### D6 能力

| RPC | 能力 |
|:--|:--|
| `RevokeCert` / `RotateCrl` / `Tidy` | `pki:cert:revoke` |
| `GetCertStatus` / `GetCrl` | `pki:cert:read` |

`pki:cert:revoke` 是新的写能力（原 `coord:pki:revoke` 废弃项由它替代）。

### D7 serial 口径对齐

此前记录 serial 是随机 64 位而与证书内嵌 serial 无关，CRL/OCSP/第三方验签无法引用。
本 ADR 起：签发时显式写 `params.serial_number`，记录 serial **逐字等于**证书内嵌 serial
（16 字节随机、首字节非零、最高位清零）。旧记录读取时以证书内嵌值为准（解析失败才回退）。

### D8 明确不做

- **OCSP**：需要常在线 responder 与独立可用性/授权面；CRL + `VerifyCert` 已覆盖当前触发条件。
- **HTTP CRL 分发端点**：coord 的对外面是 gRPC；再造一个 HTTP 面等于新增部署与鉴权单元。
- **delta CRL / CRL 缓存服务**：消费方按 `next_update` 自行刷新即可。
- **吊销账本回收 / 证书 hold 语义**：`certificateHold` 只作为 reason 值记录，不实现
  「解除 hold」的状态机。
- **HSM / PKCS#11**：`security.md` §3.3 的触发条件未全部命中，B-PKI-1 ③ 继续有效。

## 后果

- **安全**：吊销从「消费方私有账本」升级为 CA 侧权威事实——`VerifyCert` 与 CRL 消费者
  看到同一份数据；但**不查 CRL 也不调 `VerifyCert` 的第三方 TrustManager 仍会接受旧证书**
  （这是 CRL 模型的固有边界，不是实现缺口）。
- **可用性**：`GetCrl` 在吊销集合变化时写一次 CRL 状态（CAS）；`RotateCrl` 总是写。
  没有后台发布任务 ⇒ CRL 的「新鲜度」由调用方的拉取节奏决定。
- **运维**：备份必须覆盖 `/_pki/v1/revoked/` 与 `/_pki/v1/crl`（与 CA 同一 KV 备份即可）；
  `Tidy` 的 cadence 由运维决定（不自动跑），默认保留期 24h。
- **契约**：`coord.pki.v1` 新增 5 个 RPC、2 个枚举与对应消息（wire Minor）；新增能力
  `pki:cert:revoke`；`VerifyCert` 的判据多一条「未被吊销」（行为收紧）。
- **兼容**：按仓库口径不保留双路径；旧记录（记录 serial ≠ 内嵌 serial）仍可按内嵌 serial
  吊销与写入 CRL，但按旧记录 serial 查 `GetCertStatus` 不会被当作内嵌 serial 的别名。

## 替代方案

- **维持 #375 的 seedj 私有账本**：第三方验签方永远看不到吊销，且每个消费方要自己实现
  CRL/状态机。触发条件已命中，否决。
- **只加 `RevokeCert`、不出 CRL**：`VerifyCert` 能看到，但第三方 TrustManager 没有离线
  可验的吊销材料；等于把「必须在线调 coord」写进 TLS 握手。否决。
- **OCSP responder**：实时性更好，但要多一个高可用在线面与签名/缓存协议；当前规模不划算。
  留作未来触发条件（在线校验成为硬要求）时的增量。
