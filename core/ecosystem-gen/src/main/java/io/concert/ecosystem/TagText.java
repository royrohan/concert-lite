package io.concert.ecosystem;

import io.concert.model.pure.SourceLocation;
import io.concert.model.pure.TaggedValue;
import java.util.Map;

/**
 * Maps a position inside a tagged value's text to its {@code file:line:col} in the source, so errors
 * point at the offending entry of a long {@code transitions} or {@code locks} value (which may span
 * lines). Escapes in the literal are accounted for.
 */
final class TagText {

    private final Map<String, String> sources;

    TagText(Map<String, String> sources) {
        this.sources = sources;
    }

    /** Location of character {@code offset} of the (unescaped) value; the tag's location if unknown. */
    SourceLocation at(TaggedValue tag, int offset) {
        SourceLocation quote = tag.location();
        if (quote == null) {
            return null;
        }
        String src = sources.get(quote.file());
        if (src == null) {
            return quote;
        }
        int line = 1;
        int i = 0;
        while (i < src.length() && line < quote.line()) {
            if (src.charAt(i++) == '\n') {
                line++;
            }
        }
        i += quote.column() - 1; // at the opening quote
        int col = quote.column();
        i++;
        col++;
        for (int n = 0; n < offset && i < src.length(); n++) {
            char c = src.charAt(i);
            int width = c == '\\' ? 2 : 1;
            if (c == '\n') {
                line++;
                col = 1;
            } else {
                col += width;
            }
            i += width;
        }
        return new SourceLocation(quote.file(), line, col);
    }
}
