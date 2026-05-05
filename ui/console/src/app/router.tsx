import {
  createRootRoute,
  createRoute,
  createRouter,
} from '@tanstack/react-router';
import { Shell } from '@/components/feature/shell';
import { OverviewPage } from '@/app/overview';
import { ClusterPage } from '@/app/cluster';
import { RegistryPage } from '@/app/registry';
import { ConfigsPage } from '@/app/configs';
import { LocksPage } from '@/app/locks';
import { WorkflowsPage } from '@/app/workflows';
import { IdGenPage } from '@/app/idgen';
import { TransitPage } from '@/app/transit';
import { PkiPage } from '@/app/pki';
import { SecurityPage } from '@/app/security';
import { BackupPage } from '@/app/backup';
import { AuditPage } from '@/app/audit';
import { NotFoundPage } from '@/app/not-found';

const rootRoute = createRootRoute({ component: Shell, notFoundComponent: NotFoundPage });

/**
 * Route tree — intentionally flat for the Batch-6 scaffold. Detail
 * pages (cluster/node/:id, configs/:key, …) are additive and land in
 * later sprints without touching the shell.
 */
const routes = [
  createRoute({ getParentRoute: () => rootRoute, path: '/', component: OverviewPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/cluster', component: ClusterPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/registry', component: RegistryPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/configs', component: ConfigsPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/locks', component: LocksPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/workflows', component: WorkflowsPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/idgen', component: IdGenPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/transit', component: TransitPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/pki', component: PkiPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/security', component: SecurityPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/backup', component: BackupPage }),
  createRoute({ getParentRoute: () => rootRoute, path: '/audit', component: AuditPage }),
];

const routeTree = rootRoute.addChildren(routes);

export const router = createRouter({
  routeTree,
  defaultPreload: 'intent',
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
