package io.concert.store;

/**
 * Service-provider interface: one implementation per backend, registered in
 * {@code META-INF/services/io.concert.store.StateStoreProvider}. Put a backend's module on the
 * runtime classpath and it becomes selectable with {@code STORE_KIND}.
 */
public interface StateStoreProvider {

    /** Values of STORE_KIND this provider serves. */
    java.util.Set<String> kinds();

    /** Builds the store from environment variables; creates tables if STORE_INIT_SCHEMA is true. */
    StateStore create(String kind);
}
