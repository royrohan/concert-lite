package io.concert.ecosystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** A small valid ecosystem model, and helpers to point at positions in it. */
final class TestModels {
    private TestModels() {}

    static final String THING = """
            ###Pure
            Enum t::Status
            {
              OPEN, DONE, GONE
            }
            Class <<concert::sm.root>>
            {
              concert::sm.type = 'thing',
              concert::sm.transitions = 'OPEN -finish-> DONE : Finish;
                OPEN -drop-> GONE : Drop',
              concert::sm.locks = 'finish: user:{ownerId}'
            }
            t::Thing
            {
              thingId: String[1];
              status: Status[1] = Status.OPEN;
              ownerId: String[0..1];
            }
            Class <<concert::sm.command>> t::Finish
            {
              ownerId: String[1];
              score: Integer[0..1];
            }
            Class <<concert::sm.command>> t::Drop
            {
              reason: String[1];
            }
            """;

    /** A small valid event-style model ({@code concert::event}): a keyed state and two events of domain shop. */
    static final String EVT = """
            ###Pure
            Enum s::Status
            {
              NEW, DONE
            }
            Class <<concert::event.state>> { concert::event.key = 'order:{orderId}' } s::Order
            {
              orderId: String[1];
              status: Status[1] = Status.NEW;
            }
            Class <<concert::event.event>>
            {
              concert::event.domain = 'shop',
              concert::event.locks = 'order:{orderId}, customer:{customerId}',
              concert::event.emits = 'Placed'
            }
            s::Create
            {
              orderId: String[1];
              customerId: String[1];
              tags: String[*];
            }
            Class <<concert::event.event>>
            {
              concert::event.domain = 'shop',
              concert::event.locks = 'order:{orderId}',
              concert::event.onError = 'NON_BLOCKING',
              concert::event.retries = '0',
              concert::event.name = 'Placed'
            }
            s::OrderPlaced
            {
              orderId: String[1];
            }
            """;

    /** {@code m.pure:<line>:<col>} of the first {@code needle} on 1-based line {@code line} of {@code src}. */
    static String pos(String src, int line, String needle) {
        String text = src.lines().skip(line - 1).findFirst().orElseThrow();
        int col = text.indexOf(needle);
        if (col < 0) {
            throw new IllegalArgumentException(needle + " not on line " + line + ": " + text);
        }
        return "m.pure:" + line + ":" + (col + 1);
    }

    static Path dir(Path tmp, Map<String, String> files) {
        try {
            Path d = Files.createDirectories(tmp.resolve("models"));
            for (Map.Entry<String, String> f : files.entrySet()) {
                Files.writeString(d.resolve(f.getKey()), f.getValue(), StandardCharsets.UTF_8);
            }
            return d;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Ecosystem load(Path tmp, String src) {
        return Ecosystem.load(dir(tmp, Map.of("m.pure", src)), "things", "io.concert.eco.things");
    }
}
