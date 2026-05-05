import { JsonDumpPage } from '@/components/feature/json-dump-page';

/**
 * Security control plane (§4.10) — exposes seal/init status, role
 * bindings and AppRole inventory at a glance. The scaffold renders
 * the raw `/api/v1/security/status` payload; login / seal / unseal /
 * rotate-shares actions arrive in a later sprint with the required
 * confirmation + 2-step risk dialogs (see ui-design-spec §5.4).
 */
export function SecurityPage() {
  return (
    <JsonDumpPage
      domain="security"
      i18nTitleKey="nav.security"
      apiPath="/api/v1/security/status"
      refetchMs={5_000}
    />
  );
}
