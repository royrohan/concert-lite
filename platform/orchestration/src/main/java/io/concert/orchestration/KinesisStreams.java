package io.concert.orchestration;

import java.util.List;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

/**
 * Creates and deletes the per-flow ingest streams ({@code concert-<flow>}, {@link io.concert.common.Flows}). The
 * coordinator of a flow ensures its stream on startup, and the trace UI's Start / Delete flow does the same, so a
 * flow brought up either way has its stream.
 */
public final class KinesisStreams {
    private KinesisStreams() {}

    /** Creates the stream if it is missing and waits until it is ACTIVE; returns true if it was created. */
    public static boolean ensure(KinesisClient kinesis, String stream, int shards, long timeoutMillis) {
        boolean created = false;
        if (status(kinesis, stream) == null) {
            try {
                kinesis.createStream(b -> b.streamName(stream).shardCount(shards));
                created = true;
            } catch (ResourceInUseException raced) {
                // created concurrently
            }
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (status(kinesis, stream) != StreamStatus.ACTIVE) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("stream " + stream + " not ACTIVE after " + timeoutMillis + " ms");
            }
            sleep(250);
        }
        return created;
    }

    /** Deletes the stream; returns false if it did not exist. */
    public static boolean delete(KinesisClient kinesis, String stream) {
        try {
            kinesis.deleteStream(b -> b.streamName(stream).enforceConsumerDeletion(true));
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    /** The stream's status, or {@code null} if it does not exist. */
    public static StreamStatus status(KinesisClient kinesis, String stream) {
        try {
            return kinesis.describeStreamSummary(b -> b.streamName(stream)).streamDescriptionSummary().streamStatus();
        } catch (ResourceNotFoundException e) {
            return null;
        }
    }

    /** Every stream whose name starts with the prefix. */
    public static List<String> list(KinesisClient kinesis, String prefix) {
        return kinesis.listStreamsPaginator(b -> b.limit(100)).streamNames().stream().filter(s -> s.startsWith(prefix)).toList();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
