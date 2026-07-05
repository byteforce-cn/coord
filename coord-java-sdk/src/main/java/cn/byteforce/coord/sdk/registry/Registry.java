package cn.byteforce.coord.sdk.registry;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Public API for service registration and discovery.
 */
public interface Registry {
    Registration register(String serviceName, String instanceId,
                          String metadata, int ttlSeconds) throws CoordException;

    List<ServiceInstance> discover(String serviceName) throws CoordException;

    DiscoverResult discoverWithRevision(String serviceName) throws CoordException;

    WatchSubscription watch(String serviceName, RegistryListener listener) throws CoordException;

    WatchSubscription watchFrom(String serviceName, long startRevision,
                                RegistryListener listener) throws CoordException;
}
