import { JsonDumpPage } from '@/components/feature/json-dump-page';
export function LocksPage() {
  return <JsonDumpPage domain="lock" i18nTitleKey="nav.locks" apiPath="/api/v1/locks" />;
}
