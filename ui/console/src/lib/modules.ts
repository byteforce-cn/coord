/**
 * Module registry.
 *
 * Mirrors `doc/ui-design-spec.md` §3 left-sidebar order. Each entry is
 * the single source of truth for:
 *   - route path (`/cluster`, `/configs`, …)
 *   - i18n key for the nav label
 *   - capability gating (shown to admins with that capability or above)
 *   - domain CSS token used by the sidebar pill and module header
 *
 * Adding a module is a one-line change here; router + sidebar consume
 * this list directly.
 */

export interface ModuleDef {
  id: string;
  path: string;
  i18nKey: string;
  domainToken: string;
  /** capability required to SEE this module at all; handlers can re-check writes. */
  readCapability?: string;
}

export const MODULES: ReadonlyArray<ModuleDef> = [
  { id: 'overview', path: '/', i18nKey: 'nav.overview', domainToken: 'cluster' },
  {
    id: 'cluster',
    path: '/cluster',
    i18nKey: 'nav.cluster',
    domainToken: 'cluster',
  },
  {
    id: 'registry',
    path: '/registry',
    i18nKey: 'nav.registry',
    domainToken: 'registry',
    readCapability: 'registry.read',
  },
  {
    id: 'configs',
    path: '/configs',
    i18nKey: 'nav.configs',
    domainToken: 'config',
    readCapability: 'config.read',
  },
  {
    id: 'locks',
    path: '/locks',
    i18nKey: 'nav.locks',
    domainToken: 'lock',
    readCapability: 'lock.read',
  },
  {
    id: 'workflows',
    path: '/workflows',
    i18nKey: 'nav.workflows',
    domainToken: 'workflow',
    readCapability: 'workflow.read',
  },
  {
    id: 'idgen',
    path: '/idgen',
    i18nKey: 'nav.idgen',
    domainToken: 'cluster',
    readCapability: 'idgen.generate',
  },
  {
    id: 'transit',
    path: '/transit',
    i18nKey: 'nav.transit',
    domainToken: 'transit',
    readCapability: 'transit.admin',
  },
  {
    id: 'pki',
    path: '/pki',
    i18nKey: 'nav.pki',
    domainToken: 'pki',
    readCapability: 'pki.read',
  },
  {
    id: 'security',
    path: '/security',
    i18nKey: 'nav.security',
    domainToken: 'security',
  },
  {
    id: 'backup',
    path: '/backup',
    i18nKey: 'nav.backup',
    domainToken: 'backup',
    readCapability: 'admin.backup',
  },
  {
    id: 'audit',
    path: '/audit',
    i18nKey: 'nav.audit',
    domainToken: 'backup',
  },
];
