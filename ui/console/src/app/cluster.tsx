import { JsonDumpPage } from '@/components/feature/json-dump-page';
export function ClusterPage() {
  return (
    <JsonDumpPage
      domain="cluster"
      i18nTitleKey="nav.cluster"
      apiPath="/api/v1/cluster/status"
      refetchMs={5_000}
    />
  );
}
