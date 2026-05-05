/**
 * Tests for the API client → CoordError contract bridge.
 *
 * The contract is enforced server-side by
 * `crates/coord-server/src/http_api/error.rs`; these tests pin the
 * mirror behaviour on the UI side so a backend rename is caught loudly.
 */

import { describe, expect, it } from 'vitest';
import { CoordApiError, createApiClient, newRequestId } from './api';

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('CoordApiError', () => {
  it('parses error body fields', () => {
    const err = new CoordApiError(429, {
      error: 'rate limited',
      code: 'rate_limit.exceeded',
      kind: 'ResourceExhausted',
      retry_after_seconds: 7,
    });
    expect(err.status).toBe(429);
    expect(err.code).toBe('rate_limit.exceeded');
    expect(err.kind).toBe('ResourceExhausted');
    expect(err.retryAfterSeconds).toBe(7);
    expect(err.message).toBe('rate limited');
  });
});

describe('createApiClient', () => {
  it('attaches X-Request-Id and Authorization headers', async () => {
    let captured: Headers | null = null;
    const fetchImpl: typeof fetch = async (_url, init) => {
      captured = new Headers(init?.headers);
      return jsonResponse(200, { ok: true });
    };
    const client = createApiClient({
      fetchImpl,
      getToken: () => 'tok-abc',
    });
    await client.get('/api/v1/healthz');
    expect(captured).not.toBeNull();
    expect(captured!.get('X-Request-Id')).toMatch(/^[0-9a-z-]+$/i);
    expect(captured!.get('Authorization')).toBe('Bearer tok-abc');
  });

  it('throws CoordApiError on 4xx with structured body', async () => {
    const fetchImpl: typeof fetch = async () =>
      jsonResponse(412, {
        error: 'sealed',
        code: 'security.sealed',
        kind: 'FailedPrecondition',
      });
    const client = createApiClient({ fetchImpl });
    await expect(client.get('/api/v1/security/seal')).rejects.toMatchObject({
      name: 'CoordApiError',
      status: 412,
      code: 'security.sealed',
      kind: 'FailedPrecondition',
    });
  });

  it('returns undefined on 204', async () => {
    const fetchImpl: typeof fetch = async () =>
      new Response(null, { status: 204 });
    const client = createApiClient({ fetchImpl });
    const out = await client.post('/api/v1/security/seal');
    expect(out).toBeUndefined();
  });
});

describe('newRequestId', () => {
  it('returns a non-empty unique-ish string', () => {
    const a = newRequestId();
    const b = newRequestId();
    expect(a).toBeTruthy();
    expect(b).toBeTruthy();
    expect(a).not.toBe(b);
  });
});
