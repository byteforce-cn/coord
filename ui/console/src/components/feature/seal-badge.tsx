import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Badge } from '@/components/ui/badge';
import { useApi } from '@/hooks/use-api';

interface SecurityStatus {
  sealed?: boolean;
  initialized?: boolean;
  [key: string]: unknown;
}

/**
 * Right-aligned persistent seal badge per ui-design-spec §3.
 * Polls `/api/v1/security/status` every 5s; a stale read is acceptable
 * since the seal state is a slow-moving invariant of the cluster.
 */
export function SealBadge() {
  const api = useApi();
  const { t } = useTranslation();
  const { data, isError, isLoading } = useQuery({
    queryKey: ['security', 'status'],
    queryFn: ({ signal }) => api.get<SecurityStatus>('/api/v1/security/status', signal),
    refetchInterval: 5_000,
  });

  if (isLoading) {
    return <Badge tone="neutral">{t('common.loading')}</Badge>;
  }
  if (isError || !data) {
    return <Badge tone="danger">{t('error.generic')}</Badge>;
  }
  if (!data.initialized) {
    return <Badge tone="warning">{t('seal.initializing')}</Badge>;
  }
  return data.sealed ? (
    <Badge tone="danger">{t('seal.sealed')}</Badge>
  ) : (
    <Badge tone="success">{t('seal.unsealed')}</Badge>
  );
}
