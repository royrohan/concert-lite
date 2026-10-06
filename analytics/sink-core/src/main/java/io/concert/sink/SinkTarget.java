package io.concert.sink;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/**
 * A database that keeps the latest snapshot of every completed entity, fed by a
 * {@link SnapshotConsumerLoop} (one consumer group per target, so a slow target does not hold up the
 * others).
 *
 * <p>Contract: {@link #apply} is idempotent and version-guarded (a snapshot never replaces one with a
 * higher or equal {@code version}), so redelivery is harmless. A target that
 * {@linkplain #storesOffsets() stores offsets} records the next offset per partition atomically with
 * the rows; the loop then resumes from {@link #storedOffsets()} rather than from Kafka's committed
 * offsets, which makes delivery effectively exactly-once.
 */
public interface SinkTarget extends AutoCloseable {

    /** Creates (or extends) the tables of {@code schema}; called once before the first {@link #apply}. */
    void ensureSchema(SchemaSpec schema);

    /**
     * Applies a batch atomically, records in partition-offset order. Throws if nothing was applied; the
     * loop then retries the same batch with backoff.
     */
    void apply(List<SnapshotRecord> batch);

    /**
     * Like {@link #apply(List)}, and also records {@code nextOffsets} (next offset per partition of
     * everything the loop read, including records it skipped or dead-lettered) with the rows, so the
     * stored offsets advance past records that are not in {@code batch}. Called with an empty batch
     * when every record of a poll was skipped. Default: {@link #apply(List)}.
     */
    default void apply(List<SnapshotRecord> batch, Map<TopicPartition, Long> nextOffsets) {
        if (!batch.isEmpty()) {
            apply(batch);
        }
    }

    /** Next offset to read per partition, as stored by the last successful {@link #apply}. */
    Map<TopicPartition, Long> storedOffsets();

    /** Whether {@link #storedOffsets()} is authoritative (else the loop relies on committed offsets). */
    default boolean storesOffsets() {
        return true;
    }

    @Override
    void close();

    /**
     * Thrown by {@link #apply} when one record's <i>content</i> makes it fail (data that cannot be
     * converted to the table's types, ...), as opposed to the database being unavailable. The loop
     * retries such a record a few times, then sends it to the dead-letter topic and applies the rest.
     * Any other exception is treated as transient and the batch is retried until it succeeds.
     */
    final class RecordException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final transient SnapshotRecord record;

        public RecordException(SnapshotRecord record, String message, Throwable cause) {
            super(message, cause);
            this.record = record;
        }

        public SnapshotRecord record() {
            return record;
        }
    }
}
