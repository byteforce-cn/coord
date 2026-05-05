import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ModulePage } from '@/components/feature/module-page';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { useApi } from '@/hooks/use-api';

interface OverviewKpi {
  [key: string]: unknown;
}

/**
 * Overview — 4.1. Aggregated cluster KPIs sourced from
 * `GET /api/v1/overview`. The endpoint already exists in
 * `crates/coord-server/src/http_api.rs` (`overview`).
 */
export function OverviewPage() {
  const api = useApi();
  const { t } = useTranslation();
  const { data, isLoading, error } = useQuery({
    queryKey: ['overview'],
    queryFn: ({ signal }) => api.get<OverviewKpi>('/api/v1/overview', signal),
    refetchInterval: 10_000,
  });

  return (
    <ModulePage
      domain="cluster"
      i18nTitleKey="nav.overview"
      description={t('app.tagline')}
    >
      {isLoading ? (
        <p className="text-sm text-fg-muted">{t('common.loading')}</p>
      ) : error ? (
        <Card>
          <CardHeader>
            <CardTitle>{t('error.generic')}</CardTitle>
            <CardDescription>{(error as Error).message}</CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Cluster KPIs</CardTitle>
            <CardDescription>
              Live data from <code>/api/v1/overview</code>
            </CardDescription>
          </CardHeader>
          <CardContent>
            <pre className="overflow-auto rounded-md bg-bg-subtle p-3 text-xs">
              {JSON.stringify(data, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}
    </ModulePage>
  );
}
