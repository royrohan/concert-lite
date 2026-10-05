package io.concert.common.api;

import java.util.List;

/**
 * @param initialPosition TRIM_HORIZON or LATEST, used for shards without a checkpoint
 * @param consumedShards closed shards fully consumed so far (carried across continue-as-new)
 * @param heartbeatTimeoutSec a dead coordinator's consumers restart elsewhere after this
 */
public record IngestConfig(
        String stream,
        String initialPosition,
        int shardListIntervalSec,
        int consumeRunMinutes,
        List<String> consumedShards,
        int heartbeatTimeoutSec) {

    public static IngestConfig defaults(String stream) {
        return new IngestConfig(stream, "TRIM_HORIZON", 30, 30, List.of(), 3);
    }

    public IngestConfig withConsumed(List<String> consumed) {
        return new IngestConfig(stream, initialPosition, shardListIntervalSec, consumeRunMinutes, consumed, heartbeatTimeoutSec);
    }
}
