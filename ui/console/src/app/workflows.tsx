import { JsonDumpPage } from '@/components/feature/json-dump-page';
export function WorkflowsPage() {
  return (
    <JsonDumpPage domain="workflow" i18nTitleKey="nav.workflows" apiPath="/api/v1/workflows" />
  );
}
