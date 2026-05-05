import { type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';

export interface ModulePageProps {
  /** Matches a domain token in `tokens.css` (cluster / config / …). */
  domain: string;
  i18nTitleKey: string;
  description?: ReactNode;
  children?: ReactNode;
}

/**
 * Standard module page chrome: colored title bar + description + body
 * slot. Keeps chrome consistent across all 12 modules so later sprints
 * can focus on body content alone.
 */
export function ModulePage({
  domain,
  i18nTitleKey,
  description,
  children,
}: ModulePageProps) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-4">
      <header className="flex items-center gap-3">
        <span
          className="h-8 w-1 rounded-sm"
          style={{ backgroundColor: `var(--color-domain-${domain})` }}
          aria-hidden
        />
        <div>
          <h1 className="text-xl font-semibold">{t(i18nTitleKey)}</h1>
          {description ? (
            <p className="text-sm text-fg-muted">{description}</p>
          ) : null}
        </div>
      </header>
      <section>{children}</section>
    </div>
  );
}
