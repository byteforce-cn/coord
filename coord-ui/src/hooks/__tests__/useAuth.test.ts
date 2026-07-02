// useAuth hooks 单元测试
// useAuth hooks 依赖 TanStack Router 的 useNavigate，完整测试需要 Router 上下文。
// 此处仅验证底层 API 调用链，完整集成测试见 Playwright E2E 测试。

import { describe, it, expect, afterAll, afterEach, beforeAll } from 'vitest'
import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'

const server = setupServer()
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterAll(() => server.close())
afterEach(() => server.resetHandlers())

describe('useAuth (API 集成)', () => {
  it('登录 API 应正确调用 POST /api/v1/auth/login', async () => {
    let capturedBody: unknown = null
    let capturedMethod = ''

    server.use(
      http.post('/api/v1/auth/login', async ({ request }) => {
        capturedMethod = request.method
        capturedBody = await request.json()
        return HttpResponse.json({
          code: 0,
          data: { policies: ['admin'], role: 'test', displayName: 'Test', tokenAccessor: 'acc', tokenTtl: 28800, tokenMaxTtl: 86400 },
          message: 'success',
        })
      })
    )

    const { api } = await import('@/api/client')
    const result = await api.auth.login('role-1', 'secret-1')

    expect(capturedMethod).toBe('POST')
    expect(capturedBody).toEqual({ roleId: 'role-1', secretId: 'secret-1' })
    expect(result.policies).toContain('admin')
  })

  it('登录失败应抛出 ApiError', async () => {
    server.use(
      http.post('/api/v1/auth/login', () =>
        HttpResponse.json({ code: 403, data: null, message: 'invalid credentials' }, { status: 200 })
      )
    )

    const { api } = await import('@/api/client')
    await expect(api.auth.login('bad', 'wrong')).rejects.toThrow('invalid credentials')
  })

  it('userinfo 应返回当前用户信息', async () => {
    server.use(
      http.get('/api/v1/auth/userinfo', () =>
        HttpResponse.json({
          code: 0,
          data: { policies: ['ui-access'], role: 'viewer', displayName: 'Viewer', tokenAccessor: 'acc', tokenTtl: 10000 },
          message: 'success',
        })
      )
    )

    const { api } = await import('@/api/client')
    const result = await api.auth.userinfo()
    expect(result.role).toBe('viewer')
    expect(result.policies).toContain('ui-access')
  })

  it('renew 应发送续期请求', async () => {
    server.use(
      http.post('/api/v1/auth/renew', () =>
        HttpResponse.json({ code: 0, data: { tokenTtl: 28800 }, message: 'success' })
      )
    )

    const { api } = await import('@/api/client')
    const result = await api.auth.renew()
    expect(result.tokenTtl).toBe(28800)
  })

  it('revoke 应发送吊销请求并返回空对象', async () => {
    server.use(
      http.post('/api/v1/auth/revoke', () =>
        HttpResponse.json({ code: 0, data: {}, message: 'success' })
      )
    )

    const { api } = await import('@/api/client')
    const result = await api.auth.revoke()
    expect(result).toEqual({})
  })
})

