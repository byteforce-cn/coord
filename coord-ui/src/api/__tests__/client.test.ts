// API 层单元测试：测试所有 registry 和 config API 方法
// 使用 vitest + MSW (Mock Service Worker) 模拟 BFF 响应

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'
import { api, ApiError } from '../client'

// ──── MSW Server 配置 ────

const server = setupServer()

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterAll(() => server.close())
afterEach(() => server.resetHandlers())

// ──── 辅助 ────

function okJson(data: unknown) {
  return HttpResponse.json({ code: 0, data, message: 'success' })
}

function errJson(code: number, message: string) {
  return HttpResponse.json({ code, data: null, message })
}

// ═══════════════════════════════════════════════
// Registry API Tests
// ═══════════════════════════════════════════════

describe('api.registry', () => {
  // ── list ──

  describe('list', () => {
    it('应返回服务列表和总数', async () => {
      server.use(
        http.get('/api/v1/registry/services', () =>
          okJson({
            services: [
              { name: 'svc-a', tags: ['v1'], status: 'passing', instances: { total: 2, healthy: 2, warning: 0, critical: 0 }, address: '10.0.0.1', port: 8080 },
              { name: 'svc-b', tags: [], status: 'critical', instances: { total: 1, healthy: 0, warning: 0, critical: 1 }, address: '10.0.0.2', port: 8081 },
            ],
            total: 2,
          })
        )
      )

      const result = await api.registry.list()
      expect(result.services).toHaveLength(2)
      expect(result.total).toBe(2)
      expect(result.services[0].name).toBe('svc-a')
    })

    it('应正确传递查询参数', async () => {
      let capturedUrl = ''
      server.use(
        http.get('/api/v1/registry/services', ({ request }) => {
          capturedUrl = request.url
          return okJson({ services: [], total: 0 })
        })
      )

      await api.registry.list({ q: 'test', status: 'passing', page: 2, pageSize: 10 })
      expect(capturedUrl).toContain('q=test')
      expect(capturedUrl).toContain('status=passing')
      expect(capturedUrl).toContain('page=2')
      expect(capturedUrl).toContain('pageSize=10')
    })

    it('status=all 时不应传 status 参数', async () => {
      let capturedUrl = ''
      server.use(
        http.get('/api/v1/registry/services', ({ request }) => {
          capturedUrl = request.url
          return okJson({ services: [], total: 0 })
        })
      )

      await api.registry.list({ status: 'all' })
      expect(capturedUrl).not.toContain('status=')
    })
  })

  // ── detail ──

  describe('detail', () => {
    it('应返回服务详情和实例列表', async () => {
      server.use(
        http.get('/api/v1/registry/services/my-service', () =>
          okJson({
            name: 'my-service',
            tags: ['prod'],
            healthRate: 100,
            instances: [
              { id: 'inst-1', address: '10.0.0.1', port: 8080, status: 'passing', lastCheck: '2026-07-01T00:00:00Z' },
            ],
          })
        )
      )

      const result = await api.registry.detail('my-service')
      expect(result.name).toBe('my-service')
      expect(result.instances).toHaveLength(1)
      expect(result.healthRate).toBe(100)
    })
  })

  // ── updateInstance ──

  describe('updateInstance', () => {
    it('应发送 PUT 请求修改实例状态', async () => {
      let capturedBody: unknown = null
      server.use(
        http.put('/api/v1/registry/services/svc/instances/inst-1', async ({ request }) => {
          capturedBody = await request.json()
          return okJson({})
        })
      )

      await api.registry.updateInstance('svc', 'inst-1', 'warning')
      expect(capturedBody).toEqual({ status: 'warning' })
    })
  })

  // ── healthCheck ──

  describe('healthCheck', () => {
    it('应发送 POST 请求触发健康检查', async () => {
      let method = ''
      server.use(
        http.post('/api/v1/registry/services/svc/health-check', ({ request }) => {
          method = request.method
          return okJson({ checked: 3 })
        })
      )

      await api.registry.healthCheck('svc')
      expect(method).toBe('POST')
    })
  })
})

// ═══════════════════════════════════════════════
// Config API Tests
// ═══════════════════════════════════════════════

describe('api.config', () => {
  // ── list ──

  describe('list', () => {
    it('应返回配置列表和总数', async () => {
      server.use(
        http.get('/api/v1/configs', () =>
          okJson({
            configs: [
              { group: 'app', key: 'db', format: 'yaml', version: 3, updatedAt: '2026-07-01T00:00:00Z', updatedBy: 'admin' },
              { group: 'app', key: 'cache', format: 'json', version: 1, updatedAt: '2026-06-01T00:00:00Z', updatedBy: 'ops' },
            ],
            total: 2,
          })
        )
      )

      const result = await api.config.list()
      expect(result.configs).toHaveLength(2)
      expect(result.total).toBe(2)
    })

    it('应正确传递 group 和 q 查询参数', async () => {
      let capturedUrl = ''
      server.use(
        http.get('/api/v1/configs', ({ request }) => {
          capturedUrl = request.url
          return okJson({ configs: [], total: 0 })
        })
      )

      await api.config.list({ group: 'app', q: 'db' })
      expect(capturedUrl).toContain('group=app')
      expect(capturedUrl).toContain('q=db')
    })
  })

  // ── detail ──

  describe('detail', () => {
    it('应返回配置详情（含版本元数据）', async () => {
      server.use(
        http.get('/api/v1/configs/app/db', () =>
          okJson({
            group: 'app', key: 'db', version: 3, createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-07-01T00:00:00Z', updatedBy: 'admin',
            changeNote: 'update pool size', format: 'yaml', data: 'host: localhost\nport: 5432',
          })
        )
      )

      const result = await api.config.detail('app', 'db')
      expect(result.group).toBe('app')
      expect(result.key).toBe('db')
      expect(result.version).toBe(3)
      expect(result.format).toBe('yaml')
      expect(result.data).toContain('host')
    })
  })

  // ── create ──

  describe('create', () => {
    it('应发送 POST 请求创建配置', async () => {
      let capturedBody: unknown = null
      server.use(
        http.post('/api/v1/configs', async ({ request }) => {
          capturedBody = await request.json()
          return okJson({ version: 1 })
        })
      )

      const result = await api.config.create({
        group: 'app', key: 'new-cfg', format: 'json',
        data: '{"port":8080}', changeNote: 'initial',
      })
      expect(result.version).toBe(1)
      expect(capturedBody).toMatchObject({ group: 'app', key: 'new-cfg' })
    })
  })

  // ── update (CAS) ──

  describe('update', () => {
    it('应发送 PUT 请求带 CAS 版本号', async () => {
      let capturedBody: unknown = null
      server.use(
        http.put('/api/v1/configs/app/db', async ({ request }) => {
          capturedBody = await request.json()
          return okJson({ version: 4 })
        })
      )

      const result = await api.config.update('app', 'db', { data: 'new', version: 3, changeNote: 'updated' })
      expect(result.version).toBe(4)
      expect(capturedBody).toMatchObject({ data: 'new', version: 3, changeNote: 'updated' })
    })
  })

  // ── delete ──

  describe('delete', () => {
    it('应发送 DELETE 请求删除配置', async () => {
      let method = ''
      server.use(
        http.delete('/api/v1/configs/app/db', ({ request }) => {
          method = request.method
          return okJson({})
        })
      )

      await api.config.delete('app', 'db')
      expect(method).toBe('DELETE')
    })
  })

  // ── versions ──

  describe('versions', () => {
    it('应返回版本历史列表', async () => {
      server.use(
        http.get('/api/v1/configs/app/db/versions', () =>
          okJson([
            { version: 3, updatedAt: '2026-07-01T00:00:00Z', updatedBy: 'admin', changeNote: 'v3' },
            { version: 2, updatedAt: '2026-06-01T00:00:00Z', updatedBy: 'admin', changeNote: 'v2' },
            { version: 1, updatedAt: '2026-01-01T00:00:00Z', updatedBy: 'admin', changeNote: 'init' },
          ])
        )
      )

      const result = await api.config.versions('app', 'db')
      expect(result).toHaveLength(3)
      expect(result[0].version).toBe(3)
    })
  })

  // ── versionDetail ──

  describe('versionDetail', () => {
    it('应返回指定版本详情', async () => {
      server.use(
        http.get('/api/v1/configs/app/db/versions/1', () =>
          okJson({ group: 'app', key: 'db', version: 1, createdAt: '2026-01-01T00:00:00Z', format: 'yaml', data: 'initial' })
        )
      )

      const result = await api.config.versionDetail('app', 'db', 1)
      expect(result.version).toBe(1)
      expect(result.data).toBe('initial')
    })
  })

  // ── rollback ──

  describe('rollback', () => {
    it('应发送 POST 请求回滚至指定版本', async () => {
      let capturedBody: unknown = null
      server.use(
        http.post('/api/v1/configs/app/db/rollback', async ({ request }) => {
          capturedBody = await request.json()
          return okJson({ newVersion: 4 })
        })
      )

      const result = await api.config.rollback('app', 'db', 2)
      expect(result.newVersion).toBe(4)
      expect(capturedBody).toEqual({ version: 2 })
    })
  })
})

// ═══════════════════════════════════════════════
// Auth API Tests
// ═══════════════════════════════════════════════

describe('api.auth', () => {
  describe('login', () => {
    it('应发送 POST 请求登录并返回用户信息', async () => {
      let capturedBody: unknown = null
      server.use(
        http.post('/api/v1/auth/login', async ({ request }) => {
          capturedBody = await request.json()
          return okJson({
            policies: ['admin'],
            role: 'console-admin',
            displayName: 'AppRole:console-',
            tokenAccessor: 'acc',
            tokenTtl: 28800,
            tokenMaxTtl: 86400,
          })
        })
      )

      const result = await api.auth.login('role-123', 'secret-456')
      expect(result.policies).toContain('admin')
      expect(result.role).toBe('console-admin')
      expect(capturedBody).toEqual({ roleId: 'role-123', secretId: 'secret-456' })
    })
  })

  describe('userinfo', () => {
    it('应返回当前用户信息', async () => {
      server.use(
        http.get('/api/v1/auth/userinfo', () =>
          okJson({ policies: ['admin'], role: 'user', displayName: 'admin', tokenAccessor: 'acc', tokenTtl: 10000 })
        )
      )

      const result = await api.auth.userinfo()
      expect(result.policies).toContain('admin')
    })
  })

  describe('renew', () => {
    it('应发送 POST 请求续期', async () => {
      server.use(
        http.post('/api/v1/auth/renew', () => okJson({ tokenTtl: 28800 }))
      )

      const result = await api.auth.renew()
      expect(result.tokenTtl).toBe(28800)
    })
  })

  describe('revoke', () => {
    it('应发送 POST 请求吊销 Token', async () => {
      server.use(
        http.post('/api/v1/auth/revoke', () => okJson({}))
      )

      await expect(api.auth.revoke()).resolves.toEqual({})
    })
  })
})

// ═══════════════════════════════════════════════
// ApiError Tests
// ═══════════════════════════════════════════════

describe('ApiError', () => {
  it('应正确构造错误对象', () => {
    const err = new ApiError(403, 'permission denied')
    expect(err.code).toBe(403)
    expect(err.message).toBe('permission denied')
    expect(err.name).toBe('ApiError')
  })

  it('HTTP 错误应抛出 ApiError', async () => {
    server.use(
      http.get('/api/v1/configs', () =>
        HttpResponse.json({ code: 500, data: null, message: 'internal error' }, { status: 500 })
      )
    )

    await expect(api.config.list()).rejects.toThrow('internal error')
  })

  it('业务错误（code != 0）应抛出 ApiError', async () => {
    server.use(
      http.get('/api/v1/configs', () =>
        HttpResponse.json({ code: 403, data: null, message: 'forbidden' }, { status: 200 })
      )
    )

    await expect(api.config.list()).rejects.toThrow('forbidden')
  })
})
