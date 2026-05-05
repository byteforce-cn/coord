/**
 * In-memory auth token store.
 *
 * Per ui-design-spec §5.6: secrets (root token, AppRole secret_id,
 * issued bearer) MUST NOT enter localStorage / sessionStorage and MUST
 * NOT be logged. The store lives in module scope, dies on page reload,
 * and intentionally does not hydrate from any persistent source.
 */

import { create } from 'zustand';

interface AuthState {
  token: string | null;
  setToken: (t: string | null) => void;
  clear: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  token: null,
  setToken: (token) => set({ token }),
  clear: () => set({ token: null }),
}));

/** Provide a stable read accessor for non-React contexts (e.g. api client). */
export function readAuthToken(): string | undefined {
  return useAuthStore.getState().token ?? undefined;
}

/**
 * Copy a sensitive string to the clipboard and schedule an automatic
 * clear after `ttlMs`. See ui-design-spec §5.6 — the 30 s countdown
 * is a hard security requirement for token / secret_id displays.
 */
export async function copySecretWithSelfErase(
  value: string,
  ttlMs = 30_000,
): Promise<void> {
  if (!navigator.clipboard) {
    throw new Error('clipboard unavailable');
  }
  await navigator.clipboard.writeText(value);
  window.setTimeout(() => {
    // Best-effort: overwrite the clipboard. We deliberately ignore
    // rejection — if the user has navigated away the browser may deny
    // the write; that is an acceptable failure mode given the value
    // would already have been displaced by subsequent user copies.
    void navigator.clipboard?.writeText('').catch(() => undefined);
  }, ttlMs);
}
