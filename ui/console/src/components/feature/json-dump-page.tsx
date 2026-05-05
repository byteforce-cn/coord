import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ModulePage } from '@/components/feature/module-page';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useApi } from '@/hooks/use-api';

/**
 * Reusable JSON-dump module page used by scaffold landing pages that
 * already have a working read endpoint. Sprint-N owners replace the
 * dump with rich tables / forms without touching chrome or data flow.
 */
export interface JsonDumpPageProps {
  domain: string;
  i18nTitleKey: string;
  apiPath: string;
  description?: string;
  capability?: string;
  refetchMs?: number;
}

export function JsonDumpPage({
  domain,
  i18nTitleKey,
  apiPath,
  description,
  refetchMs,
}: JsonDumpPageProps) {
  const api = useApi();
  const { t } = useTranslation();
  const { data, isLoading, error } = useQuery({
    queryKey: ['module', apiPath],
    queryFn: ({ signal }) => api.get<unknown>(apiPath, signal),
    ...(refetchMs !== undefined ? { refetchInterval: refetchMs } : {}),
  });

  return (
    <ModulePage domain={domain} i18nTitleKey={i18nTitleKey} description={description}>
      <Card>
        <CardHeader>
          <CardTitle>{t(i18nTitleKey)}</CardTitle>
          <CardDescription>
            <code>{apiPath}</code>
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <p className="text-sm text-fg-muted">{t('common.loading')}</p>
          ) : error ? (
            <pre className="rounded-md bg-danger-subtle p-3 text-xs text-danger">
              {(error as Error).message}
            </pre>
          ) : (
            <pre className="overflow-auto rounded-md bg-bg-subtle p-3 text-xs">
              {JSON.stringify(data, null, 2)}
            </pre>
          )}
        </CardContent>
      </Card>
    </ModulePage>
  );
}
