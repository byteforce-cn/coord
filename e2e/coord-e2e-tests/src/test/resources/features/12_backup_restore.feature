# language: zh-CN
@slow
功能: 全量备份与恢复

  背景:
    假如 Coord 集群已启动
    假如 安全域已初始化且已解封

  场景: 创建全量备份返回有效 payload
    假如 配置 key="backup.test.key" value="backup-value-1" 已写入
    当 调用 CreateBackup
    那么 返回 payload_json 非空
    并且 created_unix_ms > 0

  场景: 恢复备份后数据可访问
    假如 配置 key="backup.restore.key" value="restore-check" 已写入
    当 调用 CreateBackup
    并且 调用 RestoreBackup 使用之前的 payload
    那么 restored=true
    当 读取配置 key="backup.restore.key"
    那么 配置值为 "restore-check"

  场景: 备份包含安全域数据
    假如 Transit密钥 "backup-transit-key" 已创建 algorithm="AES256-GCM96"
    当 加密明文 "backup-secret"
    并且 调用 CreateBackup
    并且 调用 RestoreBackup 使用之前的 payload
    那么 restored=true
    当 解密该密文
    那么 解密结果等于原明文
