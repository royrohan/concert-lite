package io.concert.model.pure;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * The built-in concert profiles ({@code concert/concert.pure} on the classpath): {@code concert::sm}, the
 * stereotypes and tags that declare state machines in Pure models, and {@code concert::event}, which declares
 * event-style events (one handler per event type) and their keyed-state documents.
 */
public final class ConcertProfile {
    private ConcertProfile() {}

    /** Qualified profile name. */
    public static final String NAME = "concert::sm";

    /** Classpath resource and the file name it is copied to (under {@code src/main/pure/concert/}). */
    public static final String RESOURCE = "concert/concert.pure";

    public static final String FILE_NAME = "concert.pure";

    /** Stereotype of a state machine's root (data) class. */
    public static final String ROOT = "root";

    /** Stereotype of an event payload class. */
    public static final String COMMAND = "command";

    public static final String TYPE = "type";
    public static final String INITIAL = "initial";
    public static final String TRANSITIONS = "transitions";
    public static final String TERMINAL = "terminal";
    public static final String LOCKS = "locks";
    public static final String STATUS_PROPERTY = "statusProperty";
    public static final String ID_PROPERTY = "idProperty";

    /** Qualified name of the event-style profile. */
    public static final String EVENT_PROFILE = "concert::event";

    /** Stereotype of an event-style event (payload) class. */
    public static final String EVENT = "event";

    /** Stereotype of a keyed-state class. */
    public static final String STATE = "state";

    public static final String DOMAIN = "domain";
    public static final String EVENT_LOCKS = "locks";
    public static final String ON_ERROR = "onError";
    public static final String RETRIES = "retries";
    public static final String KEY = "key";
    public static final String EVENT_NAME = "name";
    public static final String EMITS = "emits";

    /** The profiles' Pure source. */
    public static String source() {
        try (InputStream in = ConcertProfile.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The profiles parsed, reported as file {@code concert.pure}. */
    public static PureModel model() {
        return PureParser.parse(source(), FILE_NAME);
    }
}
