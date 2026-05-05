# Coord Console (Batch 6 scaffold)

React + Vite + TanStack control-plane console for `coord-server`.
Implements the structure mandated by [`doc/ui-design-spec.md`](../../doc/ui-design-spec.md):

| Concern        | Implementation                                                             |
| -------------- | -------------------------------------------------------------------------- |
| Routing        | `@tanstack/react-router` flat tree under `src/app/router.tsx`              |
| State (server) | `@tanstack/react-query` with retry policy that respects `CoordApiError`    |
| State (client) | `zustand` for in-memory token + RBAC                                       |
| Styling        | Tailwind v3 wired to CSS variables in `src/styles/tokens.css` (§2)         |
| i18n           | `react-i18next`, default `zh-CN`, English switchable                       |
| Tests          | `vitest` + `@testing-library/react` (jsdom)                                |
| Security       | In-memory token, `X-Request-Id` per request, strict CSP in `index.html`    |

## Local dev

```bash
cd ui/console
pnpm install     # or npm install
pnpm dev         # vite at :5173, proxies /api → 127.0.0.1:9091
```

The dev server proxies `/api`, `/metrics`, `/healthz` to a locally
running `coord-server` (default `127.0.0.1:9091`). Adjust the proxy in
[`vite.config.ts`](vite.config.ts) if you bind a non-default port.

## Production build

```bash
pnpm build       # → dist/
```

`dist/` is consumed by `coord-server` at runtime via
[`http_api::ui::ui_index`](../../crates/coord-server/src/http_api/ui.rs).
The `/ui` route serves `index.html`; static assets are mounted under
`/ui/*` and stream from disk.

## Tests

```bash
pnpm test        # vitest run, jsdom env
pnpm typecheck   # tsc --noEmit
```

Initial coverage focuses on the API ↔ CoordError contract and the RBAC
primitives so a server-side rename of `code` / `kind` / `capabilities`
breaks the suite loudly.

## Module status

All 12 modules from spec §3 have a route, sidebar entry, and either a
working JSON read or an explicit "deferred" placeholder with operator
guidance. Per §7 of the spec, write actions and rich tables land in
later sprints; the scaffold guarantees the chrome (shell, theme, i18n,
RBAC, error contract) is in place so each subsequent feature touches
its own page only.

## Conventions enforced

* No hard-coded colors or pixel values — every visual primitive comes
  from `src/styles/tokens.css` via Tailwind utilities or `var(...)`.
* Secrets never enter `localStorage` / `sessionStorage` (see
  [`src/lib/auth-store.ts`](src/lib/auth-store.ts) `copySecretWithSelfErase`).
* Server errors surface as [`CoordApiError`](src/lib/api.ts) carrying
  the stable `code` / `kind` / `retry_after_seconds` returned by
  `crates/coord-server/src/http_api/error.rs`.
