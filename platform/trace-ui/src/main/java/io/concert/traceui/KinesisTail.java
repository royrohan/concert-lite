package io.concert.traceui;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
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
 * The publisher side of the flow view: tails the ingest stream from LATEST (independently of the
 * coordinator, which checkpoints its own position) and counts records per flow into one-second
 * buckets by the time this tail read them, so "published/s" is what producers put on the stream, not
 * what the coordinator has read. Read time rather than {@code ApproximateArrivalTimestamp}: polling
 * every second keeps it within about a second of the put, and LocalStack's arrival stamps drift
 * minutes behind the wall clock, which would push every record out of the window. Started with the trace UI; polls each shard once a second (well under
 * the 5 reads/s per shard limit) and re-lists shards every 30s, so resharding and new streams heal.
 */
final class KinesisTail implements AutoCloseable {
    /** Buckets kept: the longest flow-view window. */
    static final int HISTORY_SEC = 3600;

    private final KinesisClient kinesis;
    private final String stream;
    private final Map<String, String> iterators = new ConcurrentHashMap<>();
    /** Per flow: [epochSecond, count] buckets, oldest first. */
    private final Map<String, Deque<long[]>> buckets = new HashMap<>();
    private final Map<String, Long> millisBehind = new TreeMap<>();
    private volatile Thread poller;
    private volatile String error;
    private volatile long startedAt;
    private volatile boolean closed;

    KinesisTail(KinesisClient kinesis, String stream) {
        this.kinesis = kinesis;
        this.stream = stream;
    }

    String stream() {
        return stream;
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
                error = "Kinesis stream " + stream + " not readable: " + e.getMessage();
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
        for (Shard s : kinesis.listShards(r -> r.streamName(stream)).shards()) {
            boolean open = s.sequenceNumberRange().endingSequenceNumber() == null;
            if (open && !iterators.containsKey(s.shardId())) {
                iterators.put(s.shardId(), kinesis.getShardIterator(r -> r.streamName(stream).shardId(s.shardId())
                        .shardIteratorType(ShardIteratorType.LATEST)).shardIterator());
            }
        }
    }

    private void poll(String shard) {
        String it = iterators.get(shard);
        GetRecordsResponse resp = kinesis.getRecords(r -> r.shardIterator(it).limit(10_000));
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
        out.put("stream", stream);
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
