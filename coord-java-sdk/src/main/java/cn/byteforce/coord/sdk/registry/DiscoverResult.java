package cn.byteforce.coord.sdk.registry;

import java.util.List;

/** Discover result containing service instances and the current global revision. */
public final class DiscoverResult {
    private final List<ServiceInstance> instances;
    private final long revision;

    public DiscoverResult(List<ServiceInstance> instances, long revision) {
        this.instances = List.copyOf(instances);
        this.revision = revision;
    }

    public List<ServiceInstance> instances() { return instances; }
    public long revision() { return revision; }

    @Override
    public String toString() {
        return "DiscoverResult{instances=" + instances.size() + ", revision=" + revision + "}";
    }
}
