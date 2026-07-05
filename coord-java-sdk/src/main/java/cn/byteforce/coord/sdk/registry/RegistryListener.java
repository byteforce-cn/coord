package cn.byteforce.coord.sdk.registry;

/** Listener for registry change events. */
@FunctionalInterface
public interface RegistryListener {
    void onEvent(RegistryEvent event);
}
