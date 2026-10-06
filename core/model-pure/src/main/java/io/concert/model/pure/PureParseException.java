package io.concert.model.pure;

/** A lexical or syntax error; the message is {@code file:line:col: detail}. */
public final class PureParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String detail;
    private final String file;
    private final int line;
    private final int column;

    public PureParseException(String message, String file, int line, int column) {
        super(file + ":" + line + ":" + column + ": " + message);
        this.detail = message;
        this.file = file;
        this.line = line;
        this.column = column;
    }

    /** The message without the location prefix. */
    public String detail() {
        return detail;
    }

    public String file() {
        return file;
    }

    public int line() {
        return line;
    }

    public int column() {
        return column;
    }
}
