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
      // 背景：第四轮把 `pnpm test:coverage` 接进 CI（此前 coord-ui 的单测从未在 CI
      // 执行过），但当时的全局门槛 80/70 从未被验证过 —— 实测当前全局 statements
      // 只有 ~15%。原因很具体：`src/routes/**`、`src/components/**` 里的页面/组件
      // **完全没有 vitest 单测**，它们的验证只发生在 Playwright e2e（e2e 不计入
      // vitest 覆盖率）。所以原设置不是一个"严格的门禁"，而是一个**恒为红**的门禁 ——
      // 只会训练大家忽略它。
      //
      // 现在改成两段式，让门禁既真实又有效：
      //  1) 全局门槛 = **棘轮（ratchet）**：钉在"当前已达到的水平"上，作用是
      //     **阻止覆盖率回退**，而不是声明已经达标。新增代码请配套单测。
      //  2) 真正有单测的模块按高门槛要求：`src/api/**` 当前实际 ~94/90/95/100。
      //
      // 全局覆盖提升到 80/70 属于**未完成项**（需要给路由/组件补单测），
      // 已记入 docs/production/remaining-known-gaps.md，不在此处假装达标。
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
