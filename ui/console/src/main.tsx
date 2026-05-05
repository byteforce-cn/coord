/**
 * Console entry point.
 *
 * Bootstraps the React tree with the four cross-cutting providers
 * required by every page (per ui-design-spec §1):
 *   1. React Query (server cache, retry policy)
 *   2. ApiProvider (in-memory token, request-id, CoordApiError parsing)
 *   3. i18next (zh-CN default, en switchable)
 *   4. TanStack Router (route tree from `@/app/router`)
 *
 * Strict-mode is enabled so dev double-renders surface accidental
 * effect dependencies early. Production builds are unaffected.
 */

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { router } from '@/app/router';
import { ApiProvider } from '@/hooks/use-api';
import { CoordApiError } from '@/lib/api';
import './i18n';
import './styles/globals.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error) => {
        // Per spec §5.5: do not auto-retry 4xx (the user must act).
        if (error instanceof CoordApiError && error.status >= 400 && error.status < 500) {
          return false;
        }
        return failureCount < 2;
      },
      refetchOnWindowFocus: false,
      staleTime: 5_000,
    },
    mutations: {
      retry: false,
    },
  },
});

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('coord-console: #root not found in index.html');
}

createRoot(rootEl).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ApiProvider>
        <RouterProvider router={router} />
      </ApiProvider>
    </QueryClientProvider>
  </StrictMode>,
);
