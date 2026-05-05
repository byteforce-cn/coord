/**
 * Coord API client — thin `fetch` wrapper.
 *
 * Contract with `coord-server`:
 *   - JSON-only; requests and responses serialize through `JSON.stringify`.
 *   - Bearer token (root or AppRole-issued) injected via `Authorization`.
 *   - Every request carries an `X-Request-Id` for correlation with
 *     server-side spans / audit log (see doc/ui-design-spec.md §5.6).
 *   - Server-side errors surface as `{ error, code?, kind?,
 *     retry_after_seconds? }` (see crates/coord-server/src/http_api/
 *     error.rs). Client exposes them as `CoordApiError` so TanStack
 *     Query error boundaries can translate `code` → i18n key.
 */

export interface CoordErrorBody {
  error: string;
  code?: string;
  kind?: string;
  retry_after_seconds?: number;
}

export class CoordApiError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  readonly kind: string | undefined;
  readonly retryAfterSeconds: number | undefined;

  constructor(status: number, body: CoordErrorBody) {
    super(body.error || `HTTP ${status}`);
    this.name = 'CoordApiError';
    this.status = status;
    this.code = body.code;
    this.kind = body.kind;
    this.retryAfterSeconds = body.retry_after_seconds;
  }
}

export interface ApiOptions {
  /** In-memory bearer token. Never persisted to localStorage (per §5.6). */
  getToken?: () => string | undefined;
  /** Override for tests. */
  fetchImpl?: typeof fetch;
  /** Defaults to relative `''` so the build is served from the same origin. */
  baseUrl?: string;
}

export interface RequestInitLike {
  method?: string;
  json?: unknown;
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

/** Crypto-grade request-id; falls back to Math.random if crypto is unavailable. */
export function newRequestId(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  // Non-cryptographic fallback; request-id only needs to be unique within
  // a rolling window, not unpredictable.
  return `req-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function createApiClient(opts: ApiOptions = {}) {
  const fetchImpl = opts.fetchImpl ?? globalThis.fetch.bind(globalThis);
  const baseUrl = opts.baseUrl ?? '';

  async function request<T>(
    path: string,
    init: RequestInitLike = {},
  ): Promise<T> {
    const requestId = newRequestId();
    const headers: Record<string, string> = {
      Accept: 'application/json',
      'X-Request-Id': requestId,
      ...(init.headers ?? {}),
    };
    const token = opts.getToken?.();
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }
    let body: BodyInit | undefined;
    if (init.json !== undefined) {
      headers['Content-Type'] = 'application/json';
      body = JSON.stringify(init.json);
    }

    const res = await fetchImpl(`${baseUrl}${path}`, {
      method: init.method ?? 'GET',
      headers,
      ...(body !== undefined ? { body } : {}),
      ...(init.signal ? { signal: init.signal } : {}),
      credentials: 'same-origin',
    });

    if (!res.ok) {
      let errBody: CoordErrorBody;
      try {
        errBody = (await res.json()) as CoordErrorBody;
      } catch {
        errBody = { error: `HTTP ${res.status}` };
      }
      throw new CoordApiError(res.status, errBody);
    }
    if (res.status === 204) {
      return undefined as T;
    }
    return (await res.json()) as T;
  }

  return {
    request,
    get: <T>(path: string, signal?: AbortSignal) =>
      request<T>(path, signal ? { signal } : {}),
    post: <T>(path: string, json?: unknown, signal?: AbortSignal) =>
      request<T>(path, {
        method: 'POST',
        ...(json !== undefined ? { json } : {}),
        ...(signal ? { signal } : {}),
      }),
  };
}

export type ApiClient = ReturnType<typeof createApiClient>;
