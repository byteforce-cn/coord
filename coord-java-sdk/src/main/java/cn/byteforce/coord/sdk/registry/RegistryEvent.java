package cn.byteforce.coord.sdk.registry;

import java.util.List;
import java.util.Objects;

/** An event describing a change in service instances. */
public final class RegistryEvent {
    public enum EventType {
        INSTANCES_ADDED,
        INSTANCES_REMOVED,
        INSTANCES_UPDATED
    }

    private final EventType type;
    private final List<ServiceInstance> instances;
    private final long revision;

    public RegistryEvent(EventType type, List<ServiceInstance> instances, long revision) {
        this.type = Objects.requireNonNull(type, "type");
        this.instances = List.copyOf(instances);
        this.revision = revision;
    }

    public EventType getType() { return type; }
    public List<ServiceInstance> getInstances() { return instances; }
    public long getRevision() { return revision; }

    @Override
    public String toString() {
        return "RegistryEvent{type=" + type + ", instances=" + instances.size() + ", revision=" + revision + "}";
    }
}
