import { JsonDumpPage } from '@/components/feature/json-dump-page';
export function ConfigsPage() {
  return <JsonDumpPage domain="config" i18nTitleKey="nav.configs" apiPath="/api/v1/configs" />;
}
