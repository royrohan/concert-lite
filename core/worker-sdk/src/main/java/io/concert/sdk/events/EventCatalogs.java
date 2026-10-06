package io.concert.sdk.events;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Process-wide registry of {@link EventCatalog}s: the ones {@link #register registered} explicitly plus every
 * catalog on the class path ({@link ServiceLoader}, loaded once). Lookups are used by handlers' {@code emit}
 * (also inside entity workflow code, so registrations must be the same on every worker of a deployment).
 */
public final class EventCatalogs {
    private EventCatalogs() {}

    private static final List<EventCatalog> catalogs = new CopyOnWriteArrayList<>();
    private static final Map<Class<?>, EventCatalog.EventType> byClass = new ConcurrentHashMap<>();
    private static final Map<String, EventCatalog.EventType> byDomainAndName = new ConcurrentHashMap<>();
    private static final Map<Class<?>, EventCatalog.StateType> statesByClass = new ConcurrentHashMap<>();
    private static volatile boolean serviceLoaded;

    /** Registers a catalog; types already registered (same class) are replaced. Returns the catalog. */
    public static synchronized EventCatalog register(EventCatalog catalog) {
        catalogs.add(catalog);
        for (EventCatalog.EventType t : catalog.events()) {
            byClass.put(t.payloadType(), t);
            byDomainAndName.put(t.domain() + "/" + t.name(), t);
        }
        for (EventCatalog.StateType s : catalog.states()) {
            statesByClass.put(s.type(), s);
        }
        return catalog;
    }

    public static List<EventCatalog> all() {
        loadServices();
        return List.copyOf(catalogs);
    }

    public static Optional<EventCatalog.EventType> byClass(Class<?> payloadType) {
        loadServices();
        return Optional.ofNullable(byClass.get(payloadType));
    }

    public static Optional<EventCatalog.EventType> byName(String domain, String eventType) {
        loadServices();
        return Optional.ofNullable(byDomainAndName.get(domain + "/" + eventType));
    }

    public static Optional<EventCatalog.StateType> state(Class<?> type) {
        loadServices();
        return Optional.ofNullable(statesByClass.get(type));
    }

    /** Event types of one domain. */
    public static List<EventCatalog.EventType> forDomain(String domain) {
        loadServices();
        return byDomainAndName.values().stream().filter(t -> t.domain().equals(domain)).toList();
    }

    private static void loadServices() {
        if (!serviceLoaded) {
            synchronized (EventCatalogs.class) {
                if (!serviceLoaded) {
                    serviceLoaded = true;
                    ServiceLoader.load(EventCatalog.class).forEach(EventCatalogs::register);
                }
            }
        }
    }
}
