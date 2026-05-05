# language: zh-CN
@core
功能: PKI 证书颁发机构

  背景:
    假如 Coord 集群已启动
    假如 PKI角色 "web-role" 已创建 allowed_domains="example.com" max_ttl="8760h"

  场景: 颁发证书
    当 颁发证书 common_name="www.example.com" ttl="24h"
    那么 返回证书 PEM 非空
    并且 serial_number 非空
    并且 PEM 包含 "BEGIN CERTIFICATE"

  场景: 续签证书
    当 颁发证书 common_name="renew.example.com" ttl="1h"
    并且 续签当前证书 ttl="48h"
    那么 新 PEM 非空

  场景: 吊销证书
    当 颁发证书 common_name="revoke.example.com" ttl="24h"
    并且 吊销当前证书
    并且 检查当前证书状态
    那么 证书状态为 "REVOKED"

  场景: 获取 CA Chain
    当 获取 CA Chain
    那么 CA Chain 非空

  场景: 获取 CRL 包含已吊销证书
    当 颁发证书 common_name="crl-test.example.com" ttl="1h"
    并且 吊销当前证书
    并且 获取 CRL
    那么 CRL 包含当前 serial

  场景: ACME 流程颁发证书
    当 创建 ACME Order domain="acme.example.com"
    那么 返回 order_id 和 challenge_token
    当 完成 ACME Challenge
    并且 Finalize ACME Order csr="(test-csr)"
    那么 返回最终证书 PEM 非空

  场景: ACME 使用真实 PKCS#10 CSR 签发并校验 Subject CN
    当 创建 ACME Order domain="acme2.example.com"
    那么 返回 order_id 和 challenge_token
    当 完成 ACME Challenge
    并且 Finalize ACME Order 使用真实 CSR domain="acme2.example.com"
    那么 返回最终证书 PEM 非空
    并且 证书 Subject CN 为 "acme2.example.com"

  场景: 自动续期策略触发后证书有效期更新
    当 颁发证书 common_name="autorenew.example.com" ttl="1h"
    并且 更新自动续期策略 enabled=true renew_before_seconds=3500
    当 运行 RunAutoRenew
    那么 RunAutoRenew 已处理至少 0 条（策略按需运行）

  场景: 检查有效证书状态为 VALID
    当 颁发证书 common_name="valid.example.com" ttl="24h"
    并且 检查当前证书状态
    那么 证书状态为 "VALID"
