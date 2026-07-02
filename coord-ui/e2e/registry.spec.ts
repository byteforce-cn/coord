// Playwright E2E — 服务注册中心流程测试

import { test, expect } from '@playwright/test'

test.describe('Registry E2E', () => {
  test('应显示登录页面（未认证时访问服务列表）', async ({ page }) => {
    await page.goto('/registry')
    await expect(page).toHaveURL(/\/login/)
  })

  test('应显示登录页面（未认证时访问服务详情）', async ({ page }) => {
    await page.goto('/registry/my-service')
    await expect(page).toHaveURL(/\/login/)
  })
})
