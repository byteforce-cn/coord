export const mockUser = {
  policies: ['admin', 'ui-access'],
  role: 'console-admin',
  displayName: 'Admin User',
  tokenAccessor: 's.xxxxxxxxxxxx',
  tokenTtl: 28800,
  tokenMaxTtl: 86400,
}

export const mockServices = [
  {
    name: 'api-gateway',
    tags: ['prod', 'gateway', 'v2'],
    status: 'passing',
    instances: { total: 4, healthy: 4, warning: 0, critical: 0 },
    address: '10.0.1.10',
    port: 8080,
  },
  {
    name: 'user-service',
    tags: ['prod', 'core'],
    status: 'warning',
    instances: { total: 3, healthy: 2, warning: 1, critical: 0 },
    address: '10.0.1.20',
    port: 8081,
  },
  {
    name: 'order-service',
    tags: ['prod', 'core'],
    status: 'critical',
    instances: { total: 2, healthy: 0, warning: 0, critical: 2 },
    address: '10.0.1.30',
    port: 8082,
  },
  {
    name: 'payment-service',
    tags: ['prod', 'finance'],
    status: 'passing',
    instances: { total: 2, healthy: 2, warning: 0, critical: 0 },
    address: '10.0.1.40',
    port: 8083,
  },
]

export const mockConfigs = [
  {
    group: 'app',
    key: 'database',
    format: 'yaml',
    version: 3,
    updatedAt: '2026-07-01T10:00:00Z',
    updatedBy: 'admin',
  },
  {
    group: 'app',
    key: 'cache',
    format: 'json',
    version: 5,
    updatedAt: '2026-06-30T15:30:00Z',
    updatedBy: 'ops',
  },
  {
    group: 'service/api',
    key: 'rate-limit',
    format: 'yaml',
    version: 2,
    updatedAt: '2026-06-29T09:00:00Z',
    updatedBy: 'admin',
  },
]

export const mockConfigDetail = {
  group: 'app',
  key: 'database',
  version: 3,
  createdAt: '2026-06-01T10:00:00Z',
  updatedAt: '2026-07-01T14:30:00Z',
  updatedBy: 'admin',
  changeNote: '更新数据库连接池配置',
  format: 'yaml',
  data: 'server:\n  port: 8080\n  host: 0.0.0.0\ndatabase:\n  pool:\n    min: 5\n    max: 20\n  timeout: 30s',
}

export const mockConfigVersions = [
  { version: 3, updatedAt: '2026-07-01T14:30:00Z', updatedBy: 'admin', changeNote: '更新数据库连接池配置' },
  { version: 2, updatedAt: '2026-06-15T10:00:00Z', updatedBy: 'admin', changeNote: '调整连接池大小' },
  { version: 1, updatedAt: '2026-06-01T10:00:00Z', updatedBy: 'admin', changeNote: '初始配置' },
]

export const mockServiceDetail = {
  name: 'api-gateway',
  tags: ['prod', 'gateway', 'v2'],
  healthRate: 100,
  instances: [
    { id: 'inst-001', address: '10.0.1.10', port: 8080, status: 'passing', lastCheck: '2026-07-01T14:30:00Z' },
    { id: 'inst-002', address: '10.0.1.11', port: 8080, status: 'passing', lastCheck: '2026-07-01T14:30:00Z' },
    { id: 'inst-003', address: '10.0.1.12', port: 8080, status: 'passing', lastCheck: '2026-07-01T14:30:00Z' },
    { id: 'inst-004', address: '10.0.1.13', port: 8080, status: 'warning', lastCheck: '2026-07-01T14:28:00Z' },
  ],
}
