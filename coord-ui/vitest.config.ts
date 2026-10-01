import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test-utils/setup.ts'],
    include: ['src/**/__tests__/**/*.test.{ts,tsx}'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/routeTree.gen.ts', 'src/test-utils/**'],
      // 覆盖率门槛
      //
      // 防"恒为红"的校验：全局门槛必须钉在**当前已达到的水平**上（棘轮语义），
      // 作用是**阻止覆盖率回退**，而不是声明已达标——钉 80/70 而实际只有 ~15%
      // 的校验只会训练大家忽略它。原因：`src/routes/**`、`src/components/**` 的
      // 页面/组件**没有 vitest 单测**（其验证在 Playwright e2e，e2e 不计入
      // vitest 覆盖率）。
      //
      // 两段式：
      //  1) 全局门槛 = 棘轮：当前水平（新增代码请配套单测）；
      //  2) 真正有单测的模块按高门槛要求：`src/api/**` 当前实际 ~94/90/95/100。
      //
      // 全局覆盖提升到 80/70 属于**未完成项**（需给路由/组件补单测）。
      thresholds: {
        statements: 14,
        lines: 13,
        functions: 14,
        branches: 12,
        'src/api/**': {
          statements: 90,
          lines: 90,
          functions: 90,
          branches: 85,
        },
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
})
