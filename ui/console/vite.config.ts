import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite config for the coord control-plane console.
//
// The console is served by coord-server from `dist/` via the existing
// `/ui` route (see `http_api::ui::ui_index`). Build output is consumed
// directly by the Rust binary at runtime.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Forward API + UI backend calls to the running coord-server.
      '/api': { target: 'http://127.0.0.1:9091', changeOrigin: true },
      '/metrics': { target: 'http://127.0.0.1:9091', changeOrigin: true },
      '/healthz': { target: 'http://127.0.0.1:9091', changeOrigin: true },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    target: 'es2022',
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test-setup.ts'],
    globals: true,
    css: false,
  },
});
