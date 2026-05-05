import { JsonDumpPage } from '@/components/feature/json-dump-page';

/**
 * PKI (§4.9) — certificate inventory.
 *
 * Issue / renew / revoke / ACME flows are deferred to later sprints
 * (each requires its own form + confirm dialog with risk-appropriate
 * copy). The scaffold lights up the route + nav entry so RBAC-gated
 * users see the module exists.
 */
export function PkiPage() {
  return (
    <JsonDumpPage
      domain="pki"
      i18nTitleKey="nav.pki"
      apiPath="/api/v1/pki/certificates"
      capability="pki.read"
      refetchMs={15_000}
    />
  );
}
