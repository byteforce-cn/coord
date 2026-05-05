/**
 * Vitest jsdom setup. Imported by `vite.config.ts > test.setupFiles`.
 *
 * Adds @testing-library/jest-dom matchers (`toBeInTheDocument`, etc.)
 * and shims a handful of browser APIs that jsdom does not implement
 * but our components touch under their happy path.
 */

import '@testing-library/jest-dom/vitest';

// jsdom does not implement matchMedia; some shadcn primitives probe it.
if (typeof window !== 'undefined' && !window.matchMedia) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
      addListener: () => undefined,
      removeListener: () => undefined,
      dispatchEvent: () => false,
    }),
  });
}
