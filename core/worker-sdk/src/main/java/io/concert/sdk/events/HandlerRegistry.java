package io.concert.sdk.events;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** The handlers of one domain by event type name, with the payload class each binds to. */
public final class HandlerRegistry {

    /** A registered handler. */
    public record Entry(String eventType, Class<?> payloadType, EventHandler<?> handler) {}

    private final String domain;
    private final Map<String, Entry> byType = new LinkedHashMap<>();

    public HandlerRegistry(String domain) {
        this.domain = domain;
    }

    public String domain() {
        return domain;
    }

    /** Registers a handler under the event type of its {@link Handles} class or type argument. */
    public HandlerRegistry add(EventHandler<?> handler) {
        Handles h = handler.getClass().getAnnotation(Handles.class);
        Class<?> type = h != null ? h.value() : inferType(handler.getClass());
        if (type == null) {
            throw new IllegalArgumentException("cannot infer the event class of " + handler.getClass().getName()
                    + "; annotate it with @Handles");
        }
        String name = h != null && !h.eventType().isBlank() ? h.eventType()
                : EventCatalogs.byClass(type).map(EventCatalog.EventType::name).orElse(type.getSimpleName());
        byType.put(checkNew(name), new Entry(name, type, handler));
        return this;
    }

    /** Registers a handler (e.g. a lambda) for an explicit event type and payload class. */
    public <T> HandlerRegistry add(String eventType, Class<T> payloadType, EventHandler<? super T> handler) {
        byType.put(checkNew(eventType), new Entry(eventType, payloadType, handler));
        return this;
    }

    private String checkNew(String eventType) {
        if (byType.containsKey(eventType)) {
            throw new IllegalArgumentException("two handlers for event type " + eventType + " in domain " + domain);
        }
        return eventType;
    }

    public Entry get(String eventType) {
        return byType.get(eventType);
    }

    public Collection<Entry> entries() {
        return byType.values();
    }

    private static Class<?> inferType(Class<?> c) {
        for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
            for (Type t : k.getGenericInterfaces()) {
                if (t instanceof ParameterizedType p && p.getRawType() == EventHandler.class
                        && p.getActualTypeArguments()[0] instanceof Class<?> arg) {
                    return arg;
                }
            }
        }
        return null;
    }
}
