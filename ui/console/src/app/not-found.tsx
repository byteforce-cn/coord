import { Link } from '@tanstack/react-router';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';

/**
 * Catch-all 404. Plain text + a link back to overview; intentionally
 * minimal so it can render even if upstream providers fail.
 */
export function NotFoundPage() {
  const { t } = useTranslation();
  return (
    <div className="flex min-h-[40vh] flex-col items-center justify-center gap-3">
      <p className="text-lg font-semibold">404</p>
      <p className="text-sm text-fg-muted">
        {t('error.generic')} — route not found.
      </p>
      <Button asChild size="sm" variant="outline">
        <Link to="/">{t('nav.overview')}</Link>
      </Button>
    </div>
  );
}
