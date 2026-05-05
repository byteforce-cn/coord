import { JsonDumpPage } from '@/components/feature/json-dump-page';

/**
 * Transit (§4.8) — list managed encryption keys.
 *
 * The scaffold only renders the raw key inventory; create / rotate /
 * encrypt / decrypt actions are gated behind `transit.admin` and land
 * in subsequent sprints (drawer + form per spec §5.3).
 */
export function TransitPage() {
  return (
    <JsonDumpPage
      domain="transit"
      i18nTitleKey="nav.transit"
      apiPath="/api/v1/transit/keys"
      capability="transit.admin"
    />
  );
}
