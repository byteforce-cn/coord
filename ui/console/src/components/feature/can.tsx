import { type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useRbacStore } from '@/lib/rbac';

export interface CanProps {
  capability: string;
  /** `"hide"` (default) mirrors the spec: missing cap = rendered nothing. */
  mode?: 'hide' | 'disabled';
  children: ReactNode;
  /** Custom fallback when `mode="disabled"` wants something other than the child. */
  fallback?: ReactNode;
}

/**
 * Declarative RBAC guard. See ui-design-spec §5.4.
 *
 * `mode="hide"` is the default; use sparingly with `mode="disabled"` for
 * onboarding screens where showing the disabled control is educational.
 */
export function Can({ capability, mode = 'hide', children, fallback }: CanProps) {
  const has = useRbacStore((s) => s.has(capability));
  const { t } = useTranslation();
  if (has) return <>{children}</>;
  if (mode === 'hide') return null;
  return (
    <span
      className="inline-block opacity-50 cursor-not-allowed"
      title={t('rbac.trainingTooltip', { capability })}
      aria-disabled
    >
      {fallback ?? children}
    </span>
  );
}
