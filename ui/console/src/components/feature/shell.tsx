import { Link, Outlet, useLocation } from '@tanstack/react-router';
import { useTranslation } from 'react-i18next';
import { MODULES } from '@/lib/modules';
import { cn } from '@/lib/cn';
import { SealBadge } from '@/components/feature/seal-badge';
import { ThemeToggle } from '@/components/feature/theme-toggle';
import { LangToggle } from '@/components/feature/lang-toggle';

/**
 * Top-level shell: top bar (workspace / cmd-k / role / seal) + left
 * sidebar (modules from `MODULES`). The spec forbids inline color
 * literals — every visual treatment here resolves to a token class.
 */
export function Shell() {
  const { t } = useTranslation();
  const location = useLocation();

  return (
    <div className="flex min-h-full flex-col">
      <header className="flex h-14 items-center justify-between border-b border-border bg-bg-elevated px-4">
        <div className="flex items-center gap-3">
          <div className="h-8 w-8 rounded-md bg-accent" aria-hidden />
          <div>
            <div className="text-sm font-semibold leading-tight">
              {t('app.title')}
            </div>
            <div className="text-xs text-fg-muted">{t('app.tagline')}</div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <SealBadge />
          <LangToggle />
          <ThemeToggle />
        </div>
      </header>

      <div className="flex flex-1">
        <nav
          aria-label="primary"
          className="w-56 shrink-0 border-r border-border bg-bg-subtle p-2"
        >
          <ul className="flex flex-col gap-0.5">
            {MODULES.map((m) => {
              const active =
                m.path === '/'
                  ? location.pathname === '/'
                  : location.pathname.startsWith(m.path);
              return (
                <li key={m.id}>
                  <Link
                    to={m.path}
                    className={cn(
                      'flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors',
                      active
                        ? 'bg-bg-elevated text-fg shadow-sm'
                        : 'text-fg-muted hover:bg-bg-muted hover:text-fg',
                    )}
                  >
                    <span
                      className="h-2 w-2 rounded-full"
                      style={{
                        backgroundColor: `var(--color-domain-${m.domainToken})`,
                      }}
                      aria-hidden
                    />
                    {t(m.i18nKey)}
                  </Link>
                </li>
              );
            })}
          </ul>
        </nav>

        <main className="flex-1 bg-bg-base p-6 scrollbar-stable">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
