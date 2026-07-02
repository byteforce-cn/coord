// Playwright E2E — 配置中心完整流程测试

import { test, expect } from '@playwright/test'

test.describe('Config Center E2E', () => {
  test('应显示登录页面（未认证时访问配置页）', async ({ page }) => {
    await page.goto('/config')
    await expect(page).toHaveURL(/\/login/)
  })

  test('应显示登录页面（未认证时访问配置详情）', async ({ page }) => {
    await page.goto('/config/app/db')
    await expect(page).toHaveURL(/\/login/)
  })

  test('应显示登录页面（未认证时访问新建配置）', async ({ page }) => {
    await page.goto('/config/create')
    await expect(page).toHaveURL(/\/login/)
  })
})
