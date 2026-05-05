/**
 * Tests for the RBAC store + role-response parser.
 *
 * Pin the wildcard behaviour so a future "deny by default" tightening
 * is a deliberate, reviewed change.
 */

import { describe, expect, it, beforeEach } from 'vitest';
import { parseRoleResponse, useRbacStore } from './rbac';

describe('useRbacStore', () => {
  beforeEach(() => {
    useRbacStore.setState({ role: null });
  });

  it('returns false for any capability when no role is set', () => {
    expect(useRbacStore.getState().has('config.read')).toBe(false);
  });

  it('honors wildcard capability', () => {
    useRbacStore.getState().setRole({ label: 'leader coord-1', capabilities: ['*'] });
    expect(useRbacStore.getState().has('arbitrary.cap')).toBe(true);
  });

  it('matches an exact capability', () => {
    useRbacStore.getState().setRole({
      label: 'reader',
      capabilities: ['config.read', 'lock.read'],
    });
    expect(useRbacStore.getState().has('config.read')).toBe(true);
    expect(useRbacStore.getState().has('config.put')).toBe(false);
  });
});

describe('parseRoleResponse', () => {
  it('trims whitespace and falls back to "unknown"', () => {
    expect(parseRoleResponse('leader coord-1\n').label).toBe('leader coord-1');
    expect(parseRoleResponse('   \n').label).toBe('unknown');
  });
});
