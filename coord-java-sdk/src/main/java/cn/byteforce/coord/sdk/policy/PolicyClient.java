package cn.byteforce.coord.sdk.policy;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Policy-based authorization API (Policy Service).
 * <p>
 * Provides RBAC/ABAC policy evaluation for access control decisions,
 * Rego bundle management (CRUD via server KV), and policy decision explanation.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     PolicyClient policy = client.policy();
 *
 *     // Check a permission
 *     boolean allowed = policy.checkPermission(
 *             "user:alice", "/api/orders", "write",
 *             "{\"department\":\"sales\"}".getBytes());
 *
 *     // Evaluate a custom Rego query
 *     byte[] result = policy.evaluate("data.rbac.allow",
 *             "{\"user\":\"alice\",\"action\":\"read\"}".getBytes());
 *
 *     // Upload a Rego policy bundle
 *     BundleInfo bundle = policy.putBundle("tenant-1", "default",
 *             "my-policy", "package my.policy\n...");
 *
 *     // Explain a policy decision
 *     byte[] trace = policy.explain("tenant-1", "default",
 *             "{\"subject\":\"alice\",\"action\":\"read\"}");
 * }
 * }</pre>
 */
public interface PolicyClient {

    /**
     * Check whether a principal is allowed to perform an action on a resource.
     *
     * @param principal the subject identifier (e.g., "user:alice", "role:admin")
     * @param resource  the target resource (e.g., "/api/orders", "*")
     * @param action    the requested action (e.g., "read", "write", "delete")
     * @param context   optional JSON context for ABAC condition evaluation
     * @return true if the action is allowed
     * @throws CoordException on evaluation or communication failure
     */
    boolean checkPermission(String principal, String resource,
                            String action, byte[] context);

    /**
     * Evaluate a Rego query against the policy engine.
     * <p>
     * The {@code query} may be a data reference such as {@code "data.rbac.allow"}
     * (boolean decision) or {@code "data.rbac.granted_permissions"} (set/array of
     * permission points), or a pure expression.
     * <p>
     * <b>Result encoding</b> (JSON, UTF-8): boolean rule → {@code true}/{@code false};
     * set/collection rule → JSON array; object rule → JSON object; no match / undefined
     * rule → {@code null}. Multiple expressions in one query → array of values.
     * <p>
     * <b>Error vs deny</b>: evaluation errors (invalid query syntax, non-JSON input,
     * runtime errors) raise {@link CoordException} (gRPC {@code INVALID_ARGUMENT}) —
     * treat as a policy/input problem, not as a deny decision. A deny / no-match result
     * is returned as {@code false} or {@code null} (JSON) in a successful response.
     * <p>
     * Note: the query addresses the Rego <em>package</em> (e.g. {@code data.icps.order.allow}),
     * not a bundle/tenant key. All enabled bundles share one engine, so package names must
     * be unique across tenants/namespaces.
     *
     * @param query the Rego query string (e.g., "data.rbac.allow")
     * @param input the JSON input document for the query
     * @return JSON result (boolean / array / object / null) from the policy engine
     * @throws CoordException on evaluation (INVALID_ARGUMENT) or communication failure
     */
    byte[] evaluate(String query, byte[] input);

    /**
     * Explain a policy decision, returning a trace of the evaluation.
     *
     * @param tenantId  the tenant ID
     * @param namespace the namespace (default "default")
     * @param inputJson the JSON input document for evaluation
     * @return JSON trace of the evaluation
     * @throws CoordException on evaluation or communication failure
     */
    byte[] explain(String tenantId, String namespace, String inputJson);

    /**
     * Upload or update a Rego policy bundle to the server KV store.
     * The bundle is shared across all agents.
     *
     * @param tenantId    the tenant ID
     * @param namespace   the namespace (default "default")
     * @param name        the bundle name
     * @param regoContent the Rego source content
     * @return the created or updated bundle info
     * @throws CoordException on communication failure
     */
    BundleInfo putBundle(String tenantId, String namespace,
                         String name, String regoContent);

    /**
     * Delete a policy bundle from the server KV store.
     *
     * @param bundleId the bundle ID to delete
     * @throws CoordException on communication failure
     */
    void deleteBundle(String bundleId);

    /**
     * Enable or disable a policy bundle.
     *
     * @param bundleId the bundle ID
     * @param enabled  true to enable, false to disable
     * @throws CoordException on communication failure
     */
    void setBundleEnabled(String bundleId, boolean enabled);

    /**
     * Roll back a policy bundle to a previous version.
     * The restored content is written as a new version (current version + 1),
     * keeping the current enabled state.
     *
     * @param bundleId the bundle ID
     * @param version  the historical version to restore (>= 1)
     * @return the bundle info after rollback
     * @throws CoordException on communication failure
     */
    BundleInfo rollbackBundle(String bundleId, long version);

    /**
     * List all historical versions of a policy bundle (rollback target discovery).
     *
     * @param bundleId the bundle ID
     * @return list of version info, ordered by version ascending
     * @throws CoordException on communication failure
     */
    List<BundleVersionInfo> listBundleVersions(String bundleId);

    /**
     * List all policy bundles, optionally filtered by tenant.
     *
     * @param tenantId the tenant ID, or null/empty to list all
     * @return list of bundle info
     * @throws CoordException on communication failure
     */
    List<BundleInfo> listBundles(String tenantId);

    /**
     * Information about a policy bundle.
     */
    class BundleInfo {
        private final String bundleId;
        private final String name;
        private final String namespace;
        private final String tenantId;
        private final boolean enabled;
        private final long version;
        private final long createdAt;
        private final long updatedAt;

        public BundleInfo(String bundleId, String name, String namespace,
                          String tenantId, boolean enabled, long version,
                          long createdAt, long updatedAt) {
            this.bundleId = bundleId;
            this.name = name;
            this.namespace = namespace;
            this.tenantId = tenantId;
            this.enabled = enabled;
            this.version = version;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }

        public String getBundleId() { return bundleId; }
        public String getName() { return name; }
        public String getNamespace() { return namespace; }
        public String getTenantId() { return tenantId; }
        public boolean isEnabled() { return enabled; }
        public long getVersion() { return version; }
        public long getCreatedAt() { return createdAt; }
        public long getUpdatedAt() { return updatedAt; }

        @Override
        public String toString() {
            return "BundleInfo{bundleId='" + bundleId + "', name='" + name +
                    "', namespace='" + namespace + "', tenantId='" + tenantId +
                    "', enabled=" + enabled + ", version=" + version + '}';
        }
    }

    /**
     * A historical version of a policy bundle.
     */
    class BundleVersionInfo {
        private final long version;
        private final long createdAt;
        private final boolean current;

        public BundleVersionInfo(long version, long createdAt, boolean current) {
            this.version = version;
            this.createdAt = createdAt;
            this.current = current;
        }

        public long getVersion() { return version; }
        public long getCreatedAt() { return createdAt; }
        public boolean isCurrent() { return current; }

        @Override
        public String toString() {
            return "BundleVersionInfo{version=" + version +
                    ", createdAt=" + createdAt +
                    ", current=" + current + '}';
        }
    }
}
