import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ModulePage } from '@/components/feature/module-page';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useApi } from '@/hooks/use-api';

interface IdResponse {
  id?: string;
  [key: string]: unknown;
}

/**
 * ID Generator (§4.7) — minimal interactive scaffold over
 * `POST /api/v1/idgen/snowflake`. The request body is empty; the
 * endpoint returns a generated snowflake id. Later sprints add batch
 * mode + tenant scoping.
 */
export function IdGenPage() {
  const api = useApi();
  const { t } = useTranslation();
  const [last, setLast] = useState<string | null>(null);

  const generate = useMutation({
    mutationFn: () => api.post<IdResponse>('/api/v1/idgen/snowflake', {}),
    onSuccess: (data) => {
      const value = typeof data.id === 'string' ? data.id : JSON.stringify(data);
      setLast(value);
    },
  });

  return (
    <ModulePage domain="cluster" i18nTitleKey="nav.idgen">
      <Card>
        <CardHeader>
          <CardTitle>{t('nav.idgen')}</CardTitle>
          <CardDescription>
            <code>/api/v1/idgen/snowflake</code>
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <Button
            size="sm"
            onClick={() => generate.mutate()}
            disabled={generate.isPending}
          >
            {generate.isPending ? t('common.loading') : t('common.confirm')}
          </Button>
          {generate.error ? (
            <pre className="rounded-md bg-danger-subtle p-3 text-xs text-danger">
              {(generate.error as Error).message}
            </pre>
          ) : null}
          {last ? (
            <pre className="overflow-auto rounded-md bg-bg-subtle p-3 font-mono text-xs">
              {last}
            </pre>
          ) : null}
        </CardContent>
      </Card>
    </ModulePage>
  );
}
