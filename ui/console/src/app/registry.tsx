import { JsonDumpPage } from '@/components/feature/json-dump-page';
export function RegistryPage() {
  return <JsonDumpPage domain="registry" i18nTitleKey="nav.registry" apiPath="/api/v1/services" />;
}
