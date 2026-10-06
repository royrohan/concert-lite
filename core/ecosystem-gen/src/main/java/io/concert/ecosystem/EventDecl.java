package io.concert.ecosystem;

import io.concert.model.pure.ClassDef;
import java.util.List;
import java.util.Locale;

/**
 * One event-style event type, declared with {@code <<concert::event.event>>} on its payload class.
 *
 * @param name the {@code eventType} on the wire ({@code concert::event.name}, default the class's simple name)
 * @param cls the event (payload) class
 * @param domain the handler application: task queue {@code ev-<domain>}, processors {@code evproc:<domain>:<key>}
 * @param locks lock key templates ({@code {field}} = payload field, {@code {id}} = event id), in declaration order;
 *     empty: the platform's default {@code <domain>:<eventId>}
 * @param onError {@code BLOCKING} or {@code NON_BLOCKING}: what happens when the handler's attempts are exhausted
 * @param maxAttempts handler attempts ({@code retries + 1})
 * @param emits event type names the handler may emit ({@code concert::event.emits}), for docs, flows and samples
 */
record EventDecl(String name, ClassDef cls, String domain, List<String> locks, String onError, int maxAttempts,
        List<String> emits) {

    /** Default {@code retries} (so 3 attempts, as {@code EventCatalog.EventType.DEFAULT_ATTEMPTS}). */
    static final int DEFAULT_RETRIES = 2;

    /** Lifecycle rows' smType, {@code evt_<snake name>} (as the SDK's {@code EventRows.eventSmType}). */
    String smType() {
        return "evt_" + snake(name);
    }

    /** Analytics table of the lifecycle rows: plural(smType), as the sinks name it. */
    String table() {
        return Ecosystem.rootTable(smType());
    }

    /** {@code OrderCreateEvent} -> {@code OrderCreateHandler}; a name without the suffix gets {@code Handler} appended. */
    String handlerClass() {
        String base = name.endsWith("Event") && name.length() > "Event".length() ? name.substring(0, name.length() - 5) : name;
        return Ecosystem.pascal(base) + "Handler";
    }

    /**
     * Same rule as the SDK's {@code EventRows.snake}: {@code OrderCreateEvent} -> {@code order_create_event}, other
     * characters become {@code _}.
     */
    static String snake(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0 && sb.length() > 0 && sb.charAt(sb.length() - 1) != '_'
                        && (Character.isLowerCase(name.charAt(i - 1)) || Character.isDigit(name.charAt(i - 1))
                                || (i + 1 < name.length() && Character.isLowerCase(name.charAt(i + 1))))) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else if (Character.isLetterOrDigit(c)) {
                sb.append(c);
            } else if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '_') {
                sb.append('_');
            }
        }
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * A keyed-state document type, declared with {@code <<concert::event.state>>}: rows {@code state:<key>} with smType
     * {@code st_<snake name>}.
     *
     * @param keyTemplate renders the key from the state's own fields, e.g. {@code order:{orderId}}; {@code null}: handlers
     *     pass the key to {@code ctx.save(key, state)}
     */
    record StateDecl(String name, ClassDef cls, String keyTemplate) {

        String smType() {
            return "st_" + snake(name);
        }

        String table() {
            return Ecosystem.rootTable(smType());
        }
    }
}
