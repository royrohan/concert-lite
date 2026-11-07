package io.concert.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * The publisher side of the flow view: tails every flow's ingest stream ({@code concert-<flow>}, re-listed with the
 * shards, so a flow started later is picked up) from LATEST (independently of the
 * coordinator, which checkpoints its own position) and counts records per flow into one-second
 * buckets by the time this tail read them, so "published/s" is what producers put on the stream, not
 * what the coordinator has read. Read time rather than {@code ApproximateArrivalTimestamp}: polling
 * every second keeps it within about a second of the put, and LocalStack's arrival stamps drift
 * minutes behind the wall clock, which would push every record out of the window. Started with the dashboard; polls each shard once a second (well under
 * the 5 reads/s per shard limit) and re-lists shards every 30s, so resharding and new streams heal.
 */
final class KinesisTail implements AutoCloseable {
    /** Buckets kept: the longest flow-view window. */
    static final int HISTORY_SEC = 3600;

    private final KinesisClient kinesis;
    private final String prefix;
    /** Per {@code stream/shardId}: the next iterator. */
    private final Map<String, String> iterators = new ConcurrentHashMap<>();
    private volatile List<String> streams = List.of();
    /** Per flow: [epochSecond, count] buckets, oldest first. */
    private final Map<String, Deque<long[]>> buckets = new HashMap<>();
    private final Map<String, Long> millisBehind = new TreeMap<>();
    private volatile Thread poller;
    private volatile String error;
    private volatile long startedAt;
    private volatile boolean closed;

    /** @param prefix stream-name prefix of the streams to tail ({@link io.concert.common.Flows#STREAM_PREFIX}) */
    KinesisTail(KinesisClient kinesis, String prefix) {
        this.kinesis = kinesis;
        this.prefix = prefix;
    }

    /** The streams tailed, as last listed. */
    List<String> streams() {
        return streams;
    }

    /** The flow id of an ingest record: the entity smType, or {@code ev:<domain>} for event-style events. */
    static String flowOf(JsonNode envelope) {
        String smType = envelope.path("smType").asText("");
        if (smType.isEmpty()) {
            return "unknown";
        }
        return "event".equals(envelope.path("style").asText("")) ? "ev:" + smType : smType;
    }

    synchronized void ensureStarted() {
        if (poller == null && !closed) {
            startedAt = System.currentTimeMillis();
            poller = Thread.ofVirtual().name("kinesis-tail").start(this::run);
        }
    }

    private void run() {
        long listedAt = 0;
        while (!closed) {
            try {
                if (System.currentTimeMillis() - listedAt > 30_000) {
                    listShards();
                    listedAt = System.currentTimeMillis();
                }
                for (String shard : iterators.keySet().toArray(String[]::new)) {
                    poll(shard);
                }
                error = null;
            } catch (RuntimeException e) {
                error = "Kinesis streams " + prefix + "* not readable: " + e.getMessage();
                iterators.clear();
                listedAt = 0;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void listShards() {
        List<String> now = io.concert.orchestration.KinesisStreams.list(kinesis, prefix);
        streams = now;
        iterators.keySet().removeIf(k -> !now.contains(k.substring(0, k.lastIndexOf('/')))); // deleted flows
        for (String stream : now) {
            List<Shard> shards;
            try {
                shards = kinesis.listShards(r -> r.streamName(stream)).shards();
            } catch (software.amazon.awssdk.services.kinesis.model.ResourceInUseException
                    | software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException e) {
                continue; // being created or deleted
            }
            for (Shard s : shards) {
                boolean open = s.sequenceNumberRange().endingSequenceNumber() == null;
                String key = stream + "/" + s.shardId();
                if (open && !iterators.containsKey(key)) {
                    iterators.put(key, kinesis.getShardIterator(r -> r.streamName(stream).shardId(s.shardId())
                            .shardIteratorType(ShardIteratorType.LATEST)).shardIterator());
                }
            }
        }
    }

    private void poll(String shard) {
        String it = iterators.get(shard);
        GetRecordsResponse resp;
        try {
            resp = kinesis.getRecords(r -> r.shardIterator(it).limit(10_000));
        } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
                | software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException e) {
            iterators.remove(shard); // stream deleted (Delete flow) or iterator expired: re-listed in 30 s
            return;
        }
        long sec = System.currentTimeMillis() / 1000;
        synchronized (this) {
            for (Record rec : resp.records()) {
                String flow;
                try {
                    flow = flowOf(Json.MAPPER.readTree(rec.data().asByteArray()));
                } catch (java.io.IOException e) {
                    flow = "unknown";
                }
                add(flow, sec);
            }
            millisBehind.put(shard, resp.millisBehindLatest());
        }
        if (resp.nextShardIterator() == null) {
            iterators.remove(shard); // closed by a reshard: its children are picked up at the next listing
        } else {
            iterators.put(shard, resp.nextShardIterator());
        }
    }

    /** Counts one record in its flow's bucket for {@code sec} (read time, so never older than the newest bucket). */
    private void add(String flow, long sec) {
        Deque<long[]> d = buckets.computeIfAbsent(flow, f -> new ArrayDeque<>());
        long[] last = d.peekLast();
        if (last != null && last[0] >= sec) {
            last[1]++;
        } else {
            d.addLast(new long[] {sec, 1});
        }
        while (d.size() > HISTORY_SEC) {
            d.pollFirst();
        }
    }

    /**
     * Records published per flow in the last {@code windowSec} seconds:
     * {@code {stream, windowSec, coveredSec, byFlow: {flow: count}, series: {flow: [per-second counts, oldest first]}, error?}}.
     * {@code coveredSec} is shorter than the window until the tail has run that long.
     */
    Map<String, Object> published(int windowSec, int seriesPoints) {
        ensureStarted();
        long nowSec = System.currentTimeMillis() / 1000;
        long from = nowSec - windowSec;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("stream", prefix + "*");
        out.put("streams", streams);
        out.put("windowSec", windowSec);
        out.put("coveredSec", Math.max(1, Math.min(windowSec, (System.currentTimeMillis() - startedAt) / 1000)));
        Map<String, Long> byFlow = new TreeMap<>();
        Map<String, long[]> series = new TreeMap<>();
        long step = Math.max(1, windowSec / seriesPoints);
        synchronized (this) {
            buckets.forEach((flow, d) -> {
                long[] s = new long[seriesPoints];
                long total = 0;
                for (long[] b : d) {
                    if (b[0] > from && b[0] <= nowSec) {
                        total += b[1];
                        int idx = (int) Math.min(seriesPoints - 1, (b[0] - from - 1) / step);
                        s[idx] += b[1];
                    }
                }
                if (total > 0) {
                    byFlow.put(flow, total);
                    series.put(flow, s);
                }
            });
            out.put("millisBehindLatest", millisBehind.values().stream().mapToLong(Long::longValue).max().orElse(0));
            out.put("shards", iterators.size());
            Map<String, Integer> perStream = new TreeMap<>();
            iterators.keySet().forEach(k -> perStream.merge(k.substring(0, k.lastIndexOf('/')), 1, Integer::sum));
            out.put("shardsByStream", perStream);
        }
        out.put("byFlow", byFlow);
        out.put("series", series);
        out.put("seriesStepSec", step);
        if (error != null) {
            out.put("error", error);
        }
        return out;
    }

    @Override
    public void close() {
        closed = true;
        Thread t = poller;
        if (t != null) {
            t.interrupt();
        }
        kinesis.close();
    }
}
