import { useTranslation } from 'react-i18next';
import { ModulePage } from '@/components/feature/module-page';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';

/**
 * Backup & Restore (§4.11) — placeholder shell.
 *
 * The backing endpoints (`/api/v1/admin/backup/{create,restore}`) are
 * high-risk and require a 2-step confirm dialog plus an explicit
 * "type the cluster name" guard. The scaffold therefore intentionally
 * does NOT expose a one-click trigger; later sprints land the form +
 * the second-factor copy contract from spec §5.4.
 */
export function BackupPage() {
  const { t } = useTranslation();
  return (
    <ModulePage domain="backup" i18nTitleKey="nav.backup">
      <Card>
        <CardHeader>
          <CardTitle>{t('nav.backup')}</CardTitle>
          <CardDescription>
            High-risk operations are deferred until the 2-step confirm dialog
            ships in the next sprint.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-fg-muted">
            Endpoints under <code>/api/v1/admin/backup/*</code> are reachable via
            <code> coord-ctl</code> until the UI surface is ready.
          </p>
        </CardContent>
      </Card>
    </ModulePage>
  );
}
