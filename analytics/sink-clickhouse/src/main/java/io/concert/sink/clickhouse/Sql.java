package io.concert.sink.clickhouse;

import java.util.regex.Pattern;

/** ClickHouse SQL quoting. */
final class Sql {
    private static final Pattern PLAIN = Pattern.compile("[a-z_][a-z0-9_]*");

    private Sql() {}

    /** An identifier, back-quoted only when it is not a plain lower-case name. */
    static String id(String name) {
        return PLAIN.matcher(name).matches() ? name : "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /** An identifier, always back-quoted (safe for any name, including keywords). */
    static String quoted(String name) {
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /** A string literal. */
    static String str(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
