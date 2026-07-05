package cn.byteforce.coord.sdk.config;

/** Listener for configuration change events. */
@FunctionalInterface
public interface ConfigListener {
    void onEvent(ConfigEvent event);
}
