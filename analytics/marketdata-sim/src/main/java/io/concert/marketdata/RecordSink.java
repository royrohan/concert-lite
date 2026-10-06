package io.concert.marketdata;

/** Where the simulator writes records: Kafka in production, stdout or memory for dry runs and tests. */
public interface RecordSink extends AutoCloseable {

    void send(String topic, String key, String value);

    default void flush() {}

    @Override
    default void close() {}
}
