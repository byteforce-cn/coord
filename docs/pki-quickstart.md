# Dev PKI Quickstart

> Owner: coord maintainers ｜ Last verified: 2026-10-18（本地 `coord dev` + `coord pki` + OpenSSL 3.0）
>
> 定位：**开发 / 测试 / 内部验证**。生产信任根请用专用 PKI（边界见
> `docs/production/ops/boundaries.md` B-PKI-1）。

目标：从空目录到「拿到一张能被只认 SAN 的校验方接受的证书」，**不装 grpcurl / jq**。

## 0. 前置

- `coord` 二进制（仓库内 `cargo build -p coord`，产物 `target/debug/coord`）；
- `openssl`（仅用于下面的人工验收；CLI 本身不依赖它）。

## 1. 起 dev（终端 1）

```bash
coord dev --fresh --data-dir /tmp/coord-dev
```

日志里会出现 CA 指纹（可与导出的根证书逐字比对）：

```text
PKI CA auto-initialized: CN=coord-agent-ca sha256=51:99:6A:...
PKI root certificate: `coord pki ca --out ca.pem` (or GetCaCert); PKI is for dev/test/internal verification only
```

`coord dev` 的 Agent gRPC 默认 `127.0.0.1:19527`（`--agent-port` 可改）；
`coord pki` 的 `--addr` 默认就是它。

## 2. 取根证书并签发（终端 2）

```bash
mkdir -p /tmp/coord-dev-certs && cd /tmp/coord-dev-certs

coord pki ca --out ca.pem
coord pki issue \
  --cn orders-api \
  --dns orders-api,orders-api.svc \
  --ip 127.0.0.1 \
  --ttl 1h \
  --out-dir ./
```

产物：`orders-api.pem` / `orders-api.key`（私钥权限 `0600`）；stdout 打印
`serial` / `not_before` / `not_after`，`--json` 输出可脚本化元数据。

- 只给 `--cn`（不传 `--dns` / `--ip`）时，CN 会**派生为 DNS SAN**；CN 不是合法
  DNS 名（非 ASCII 等）⇒ `INVALID_ARGUMENT`，CLI 退出码 2。
- 同 CN 重复 `issue` 且 SAN/TTL 一致 ⇒ 幂等返回同一张证书；参数不同 ⇒ 换新
  （新 serial/私钥，旧证书 retired 保留至到期）。

## 3. 验收：两条都过才算数

```bash
# ① SAN 必须存在且与请求一致
openssl x509 -in orders-api.pem -noout -ext subjectAltName

# ② 链路 + 主机名校验
openssl verify -CAfile ca.pem -verify_hostname orders-api orders-api.pem

# ③ 负控制：错误名字必须失败（证明不是 CN 回退在“帮忙”）
openssl verify -CAfile ca.pem -verify_hostname orders-api.evil orders-api.pem   # 必须非 0 退出
```

预期 ① 输出包含 `DNS:orders-api, DNS:orders-api.svc, IP Address:127.0.0.1`。

> **`openssl verify` 单独通过不算数**：OpenSSL 在缺少 SAN 时会回退到 CN。只认 SAN 的
> 校验方（Go `crypto/x509` ≥1.15、rustls-webpki、Chrome/Firefox、Envoy/Istio）读的是
> `subjectAltName`。rustls 路径由单测钉住：
> `coord-agent/src/pki.rs::tests::test_issued_cert_passes_webpki_positive_and_fails_other_name`
> （正例 DNS/IP + `orders-api.evil` 负控制）。

实测耗时（本地 warm build，含 `coord dev` 启动到 ②通过）：**≈0.5s**。冷构建
`coord` 二进制的编译时间另计。

## 4. 指标（可见 ≠ 告警会响）

```bash
curl -s http://127.0.0.1:19528/metrics | grep pki
# coord_agent_pki_certs_active 1
# coord_agent_pki_certs_expiring_soon 1
# coord_agent_pki_expiry_warn_window_hours 6
```

端口 = `agent_port + 1`；采样周期 30s（先采再睡，签发后 ≤1 个周期即可见）。
`1h` 证书天然落在默认 6h 告警窗口内。

**注意**：dev 默认没有 Prometheus 抓这个端口，所以“指标可见”不等于
`CoordPkiCertExpiringSoon` 会触发；告警判据需要真实抓取链路。

## 5. 轮换与升级

- 轮换（同 CN、新材料）：`coord pki list --cn orders-api` 看 serial；
  gRPC 面 `coord.pki.v1.Pki/RotateCert`（未传 SAN 时继承当前证书的 SAN）。
- 升级前建立的 dev 数据目录里的旧证书**没有 SAN**；下一次 `IssueCert` 会按
  「参数一致才幂等」自动替换。也可直接 `--fresh` 或换 CN。
- 旧的 dev 数据目录若想保留：无需手工迁移，重签即可。

## 6. 清理

终端 1 `Ctrl+C`，然后 `rm -rf /tmp/coord-dev /tmp/coord-dev-certs`。

## 边界与相关文档

- 无吊销（CRL/OCSP）、非 HSM、CN 只是软身份（身份校验以 SAN 为准）：
  `docs/production/ops/boundaries.md` B-PKI-1。
- 泄露应急 / CN 轮换：`docs/production/ops/security.md` §3 /
  `docs/production/ops/runbook.md` §4.5。
- 能力与契约：`docs/production/consumer-contract.md` §9。
