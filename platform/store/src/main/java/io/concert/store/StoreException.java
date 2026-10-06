package io.concert.store;

import java.sql.SQLException;

public class StoreException extends RuntimeException {
    public StoreException(SQLException cause) {
        super(cause.getMessage(), cause);
    }
}
