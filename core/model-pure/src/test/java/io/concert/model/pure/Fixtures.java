package io.concert.model.pure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

final class Fixtures {

    private Fixtures() {}

    static PureModel parse(String name) {
        try {
            return PureParser.parse(Path.of(Fixtures.class.getResource("/fixtures/" + name).toURI()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static ResolvedModel shop() {
        return new ModelResolver().resolve(parse("shop.pure"), parse("events.pure"));
    }
}
