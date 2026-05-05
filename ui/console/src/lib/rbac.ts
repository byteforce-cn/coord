/**
 * RBAC primitives. The server returns the caller's capability set via
 * `GET /api/v1/role` (see crates/coord-server/src/http_api.rs `role`
 * handler). The UI layer reflects that set into a small store + a
 * declarative `<Can capability="…">` guard.
 *
 * Behavioural contract (per ui-design-spec §5.4):
 *   - Missing capability → element hidden by default.
 *   - Training mode (`mode="disabled"`) → element rendered disabled
 *     with explanatory tooltip, useful for onboarding screens.
 */

import { create } from 'zustand';

export interface RoleInfo {
  /** e.g. "leader coord-1" or "follower coord-2". */
  label: string;
  capabilities: ReadonlyArray<string>;
}

interface RbacState {
  role: RoleInfo | null;
  setRole: (r: RoleInfo | null) => void;
  has: (capability: string) => boolean;
}

export const useRbacStore = create<RbacState>((set, get) => ({
  role: null,
  setRole: (role) => set({ role }),
  has: (capability) => {
    const role = get().role;
    if (!role) return false;
    if (role.capabilities.includes('*')) return true;
    return role.capabilities.includes(capability);
  },
}));

/**
 * Parse the plain-text body of `/api/v1/role` into a structured role.
 *
 * The endpoint currently returns `"<role> <node_id>\n"`; capability
 * details are not yet exposed. This helper is a forward-compat shim:
 * once the handler returns JSON, swap this function for a direct
 * schema validation.
 */
export function parseRoleResponse(raw: string): RoleInfo {
  const trimmed = raw.trim();
  return {
    label: trimmed || 'unknown',
    // Until the server enumerates capabilities, assume a conservative
    // read-only view; operators with elevated tokens will override via
    // `useRbacStore.setState` after a capability probe.
    capabilities: [],
  };
}
