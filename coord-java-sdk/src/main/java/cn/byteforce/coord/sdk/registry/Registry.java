package cn.byteforce.coord.sdk.registry;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Public API for service registration and discovery.
 */
public interface Registry {
    Registration register(String serviceName, String instanceId,
                          String metadata, int ttlSeconds) throws CoordException;

    /**
     * Discover all instances of a service by exact name match.
     *
     * @param serviceName the exact service name
     * @return list of matching instances (never null)
     */
    List<ServiceInstance> discover(String serviceName) throws CoordException;

    /**
     * Discover with the current global revision for watch-from support.
     *
     * @param serviceName the exact service name
     */
    DiscoverResult discoverWithRevision(String serviceName) throws CoordException;

    /**
     * Discover all instances matching the given filter mode.
     *
     * @param serviceName the service name or prefix
     * @param filterMode  {@link FilterMode#EXACT} or {@link FilterMode#PREFIX}
     * @return list of matching instances (never null)
     */
    List<ServiceInstance> discover(String serviceName, FilterMode filterMode) throws CoordException;

    /**
     * Discover with revision using the given filter mode.
     *
     * @param serviceName the service name or prefix
     * @param filterMode  {@link FilterMode#EXACT} or {@link FilterMode#PREFIX}
     */
    DiscoverResult discoverWithRevision(String serviceName, FilterMode filterMode) throws CoordException;

    /**
     * Discover all registered service instances across all services.
     * Equivalent to {@code discover("", FilterMode.ALL)}.
     *
     * @return list of all registered instances (never null)
     */
    List<ServiceInstance> discoverAll() throws CoordException;

    /**
     * Discover all registered instances with the current global revision.
     *
     * @return discover result with all instances and current revision
     */
    DiscoverResult discoverAllWithRevision() throws CoordException;

    WatchSubscription watch(String serviceName, RegistryListener listener) throws CoordException;

    WatchSubscription watchFrom(String serviceName, long startRevision,
                                RegistryListener listener) throws CoordException;
}
