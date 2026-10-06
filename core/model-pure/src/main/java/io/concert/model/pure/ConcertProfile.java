package io.concert.model.pure;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * The built-in {@code concert::sm} profile ({@code concert/concert.pure} on the classpath): the
 * stereotypes and tags that declare state machines in Pure models.
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

    /** The profile's Pure source. */
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

    /** The profile parsed, reported as file {@code concert.pure}. */
    public static PureModel model() {
        return PureParser.parse(source(), FILE_NAME);
    }
}
