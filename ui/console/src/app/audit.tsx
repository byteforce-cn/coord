import { useTranslation } from 'react-i18next';
import { ModulePage } from '@/components/feature/module-page';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';

/**
 * Audit & Logs (§4.12) — placeholder shell.
 *
 * The server already records risk + operation audit events
 * (`http_api/audit.rs`) but does not yet expose a query endpoint;
 * tailing happens via stdout/Loki today. This placeholder reserves
 * the route + nav so the structured-search UI lands without a
 * sidebar reshuffle.
 */
export function AuditPage() {
  const { t } = useTranslation();
  return (
    <ModulePage domain="backup" i18nTitleKey="nav.audit">
      <Card>
        <CardHeader>
          <CardTitle>{t('nav.audit')}</CardTitle>
          <CardDescription>Coming in a later sprint.</CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-fg-muted">
            Audit events are emitted to stdout / OTLP today. A query API
            (<code>/api/v1/audit/search</code>) is on the roadmap; this page
            will host the structured filter + virtualized timeline once it
            ships.
          </p>
        </CardContent>
      </Card>
    </ModulePage>
  );
}
