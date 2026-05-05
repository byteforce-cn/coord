import { createContext, useContext, useMemo, type ReactNode } from 'react';
import { createApiClient, type ApiClient } from '@/lib/api';
import { readAuthToken } from '@/lib/auth-store';

const ApiCtx = createContext<ApiClient | null>(null);

export function ApiProvider({ children }: { children: ReactNode }) {
  const client = useMemo(
    () => createApiClient({ getToken: readAuthToken }),
    [],
  );
  return <ApiCtx.Provider value={client}>{children}</ApiCtx.Provider>;
}

export function useApi(): ApiClient {
  const ctx = useContext(ApiCtx);
  if (!ctx) {
    throw new Error('useApi: missing <ApiProvider> in the component tree');
  }
  return ctx;
}
