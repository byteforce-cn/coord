package cn.byteforce.coord.sdk.registry;

import java.util.Objects;

/** Represents a registered service instance. */
public final class ServiceInstance {
    private final String instanceId;
    private final String serviceName;
    private final String metadata;

    public ServiceInstance(String instanceId, String serviceName, String metadata) {
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
        this.metadata = metadata != null ? metadata : "";
    }

    public String getInstanceId() { return instanceId; }
    public String getServiceName() { return serviceName; }
    public String getMetadata() { return metadata; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServiceInstance that)) return false;
        return instanceId.equals(that.instanceId) && serviceName.equals(that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, serviceName);
    }

    @Override
    public String toString() {
        return "ServiceInstance{instanceId='" + instanceId + "', serviceName='" + serviceName + "'}";
    }
}
