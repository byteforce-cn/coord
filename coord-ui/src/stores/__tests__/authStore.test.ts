// authStore 单元测试

import { describe, it, expect, beforeEach } from 'vitest'
import { useAuthStore } from '../authStore'

describe('authStore', () => {
  beforeEach(() => {
    // Reset store to initial state before each test
    useAuthStore.setState({
      isAuthenticated: false,
      user: null,
      isLoading: true,
    })
  })

  it('初始状态应为未认证', () => {
    const state = useAuthStore.getState()
    expect(state.isAuthenticated).toBe(false)
    expect(state.user).toBeNull()
    expect(state.isLoading).toBe(true)
  })

  it('login 应设置认证状态和用户信息', () => {
    const user = {
      policies: ['admin', 'ui-access'],
      role: 'console-admin',
      displayName: 'Admin',
      tokenAccessor: 'accessor-123',
      tokenTtl: 28800,
      tokenMaxTtl: 86400,
    }

    useAuthStore.getState().login(user)

    const state = useAuthStore.getState()
    expect(state.isAuthenticated).toBe(true)
    expect(state.user).toEqual(user)
    expect(state.isLoading).toBe(false)
    expect(state.user?.policies).toContain('admin')
    expect(state.user?.role).toBe('console-admin')
  })

  it('logout 应清除认证状态和用户信息', () => {
    // First login
    useAuthStore.getState().login({
      policies: ['admin'],
      role: 'admin',
      displayName: 'Admin',
      tokenAccessor: 'acc',
      tokenTtl: 28800,
    })

    // Then logout
    useAuthStore.getState().logout()

    const state = useAuthStore.getState()
    expect(state.isAuthenticated).toBe(false)
    expect(state.user).toBeNull()
    expect(state.isLoading).toBe(false)
  })

  it('setLoading 应更新加载状态', () => {
    useAuthStore.getState().setLoading(true)
    expect(useAuthStore.getState().isLoading).toBe(true)

    useAuthStore.getState().setLoading(false)
    expect(useAuthStore.getState().isLoading).toBe(false)
  })

  it('多次 login/logout 应正确切换状态', () => {
    const user1 = { policies: ['a'], role: 'r1', displayName: 'U1', tokenAccessor: 't1', tokenTtl: 100 }
    const user2 = { policies: ['b'], role: 'r2', displayName: 'U2', tokenAccessor: 't2', tokenTtl: 200 }

    useAuthStore.getState().login(user1)
    expect(useAuthStore.getState().user?.displayName).toBe('U1')

    useAuthStore.getState().logout()
    expect(useAuthStore.getState().user).toBeNull()

    useAuthStore.getState().login(user2)
    expect(useAuthStore.getState().user?.displayName).toBe('U2')
    expect(useAuthStore.getState().isAuthenticated).toBe(true)
  })
})
