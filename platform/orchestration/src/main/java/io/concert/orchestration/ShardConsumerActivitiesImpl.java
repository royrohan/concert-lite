package io.concert.orchestration;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.api.ShardConsumerActivities;
import io.concert.common.api.ShardInfo;
import io.concert.common.api.ShardResult;
import io.concert.store.StateStore;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.ActivityCompletionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * Reads one Kinesis shard as a long-running, heartbeating Temporal activity. The heartbeat carries
 * the last dispatched sequence number, so when a worker dies Temporal retries the activity on
 * another worker from exactly there; DSQL holds the same checkpoint for supervisor restarts.
 */
public final class ShardConsumerActivitiesImpl implements ShardConsumerActivities {
    private static final Logger log = LoggerFactory.getLogger(ShardConsumerActivitiesImpl.class);

    private final KinesisClient kinesis;
    private final StateStore store;
    private final IngestDispatcher dispatcher;
    private final long idlePollMillis;
    private final long checkpointEveryMillis;

    public ShardConsumerActivitiesImpl(
            KinesisClient kinesis, StateStore store, IngestDispatcher dispatcher, long idlePollMillis, long checkpointEveryMillis) {
        this.kinesis = kinesis;
        this.store = store;
        this.dispatcher = dispatcher;
        this.idlePollMillis = idlePollMillis;
        this.checkpointEveryMillis = checkpointEveryMillis;
    }

    @Override
    public List<ShardInfo> listShards(String stream) {
        List<ShardInfo> out = new ArrayList<>();
        String next = null;
        do {
            ListShardsRequest req = next == null
                    ? ListShardsRequest.builder().streamName(stream).build()
                    : ListShardsRequest.builder().nextToken(next).build();
            ListShardsResponse resp = kinesis.listShards(req);
            for (Shard s : resp.shards()) {
                out.add(new ShardInfo(s.shardId(), s.parentShardId(), s.adjacentParentShardId()));
            }
            next = resp.nextToken();
        } while (next != null);
        return out;
    }

    @Override
    public ShardResult consumeShard(String stream, String shardId, String initialPosition, int runMinutes) {
        ActivityExecutionContext ctx = Activity.getExecutionContext();
        Optional<String> fromHeartbeat = ctx.getHeartbeatDetails(String.class);
        String seq = fromHeartbeat.orElseGet(() -> store.loadCheckpoint(stream, shardId).orElse(null));
        String savedSeq = seq;
        log.info("consuming {}/{} from {}", stream, shardId, seq == null ? initialPosition : "after " + seq);

        String iterator = iterator(stream, shardId, seq, initialPosition);
        long deadline = System.currentTimeMillis() + runMinutes * 60_000L;
        long lastSave = System.currentTimeMillis();
        long processed = 0;
        try {
            while (System.currentTimeMillis() < deadline) {
                GetRecordsResponse resp;
                String current = iterator;
                try {
                    resp = kinesis.getRecords(r -> r.shardIterator(current).limit(500));
                } catch (ExpiredIteratorException expired) {
                    iterator = iterator(stream, shardId, seq, initialPosition);
                    continue;
                }
                List<Record> records = resp.records();
                if (!records.isEmpty()) {
                    long now = System.currentTimeMillis();
                    List<EventEnvelope> batch = new ArrayList<>(records.size());
                    for (Record r : records) {
                        try {
                            batch.add(Json.read(r.data().asByteArray(), EventEnvelope.class).withIngestTs(now));
                        } catch (RuntimeException bad) {
                            log.warn("skipping unparseable record {} on {}: {}", r.sequenceNumber(), shardId, bad.getMessage());
                        }
                    }
                    dispatchWithHeartbeat(ctx, batch, seq);
                    seq = records.get(records.size() - 1).sequenceNumber();
                    processed += records.size();
                }
                ctx.heartbeat(seq);
                if (seq != null && !seq.equals(savedSeq) && System.currentTimeMillis() - lastSave >= checkpointEveryMillis) {
                    store.saveCheckpoint(stream, shardId, seq);
                    savedSeq = seq;
                    lastSave = System.currentTimeMillis();
                }
                iterator = resp.nextShardIterator();
                if (iterator == null) {
                    if (seq != null) {
                        store.saveCheckpoint(stream, shardId, seq);
                    }
                    log.info("shard {} closed after {} records", shardId, processed);
                    return new ShardResult(shardId, true, seq, processed);
                }
                if (records.isEmpty()) {
                    Thread.sleep(idlePollMillis);
                }
            }
        } catch (ActivityCompletionException cancelledOrTimedOut) {
            saveQuietly(stream, shardId, seq, savedSeq);
            throw cancelledOrTimedOut;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            saveQuietly(stream, shardId, seq, savedSeq);
            throw Activity.wrap(ie);
        }
        saveQuietly(stream, shardId, seq, savedSeq);
        return new ShardResult(shardId, false, seq, processed);
    }

    /**
     * When downstream is saturated, enqueueing a batch blocks (that is the backpressure: records wait
     * in Kinesis). Keep heartbeating meanwhile, with the last fully dispatched sequence number, so a
     * slow batch is not mistaken for a dead consumer.
     */
    private void dispatchWithHeartbeat(ActivityExecutionContext ctx, List<EventEnvelope> batch, String committedSeq) {
        Thread beater = Thread.ofVirtual().name("shard-heartbeat").start(() -> {
            try {
                while (true) {
                    Thread.sleep(1_000);
                    ctx.heartbeat(committedSeq);
                }
            } catch (InterruptedException done) {
                // batch finished
            } catch (RuntimeException cancelled) {
                log.warn("heartbeat during slow batch failed: {}", cancelled.getMessage());
            }
        });
        try {
            dispatcher.dispatchBatch(batch);
        } finally {
            beater.interrupt();
        }
    }

    private String iterator(String stream, String shardId, String afterSeq, String initialPosition) {
        if (afterSeq != null) {
            try {
                return rawIterator(stream, shardId, afterSeq, initialPosition);
            } catch (InvalidArgumentException | ResourceNotFoundException stale) {
                // The checkpoint belongs to an earlier incarnation of the stream (e.g. it was recreated):
                // start from the configured position instead of failing forever; dedupe absorbs replays.
                log.warn("checkpoint {} is not valid for {}/{} ({}); starting from {}",
                        afterSeq, stream, shardId, stale.getMessage(), initialPosition);
                return rawIterator(stream, shardId, null, initialPosition);
            }
        }
        return rawIterator(stream, shardId, null, initialPosition);
    }

    private String rawIterator(String stream, String shardId, String afterSeq, String initialPosition) {
        return kinesis.getShardIterator(r -> {
            r.streamName(stream).shardId(shardId);
            if (afterSeq != null) {
                r.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(afterSeq);
            } else {
                r.shardIteratorType(ShardIteratorType.fromValue(initialPosition));
            }
        }).shardIterator();
    }

    private void saveQuietly(String stream, String shardId, String seq, String savedSeq) {
        if (seq == null || seq.equals(savedSeq)) {
            return;
        }
        try {
            store.saveCheckpoint(stream, shardId, seq);
        } catch (RuntimeException e) {
            log.warn("final checkpoint for {} failed: {}", shardId, e.getMessage());
        }
    }
}
