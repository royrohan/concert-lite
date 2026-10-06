package io.concert.traceui;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * The Analytics tab's pipeline strip: consumer-group lag per analytics target, from Kafka's admin API,
 * and whether Redpanda Console / Deephaven are reachable.
 *
 * <ul>
 *   <li>{@code sink-duckdb}: our consumer (offsets also stored in DuckDB, committed to Kafka too);
 *   <li>{@code sink-clickhouse} and {@code sink-clickhouse-<topic>}: ClickHouse's Kafka engine queues;
 *   <li>Deephaven reads with manual partition assignment from offset 0 and commits nothing, so it has
 *       no group and no lag here (its tables show the latest data or nothing).
 * </ul>
 */
final class PipelineStatus implements AutoCloseable {

    static final String DLQ_TOPIC = "entity-snapshots.dlq";

    private final String bootstrap;
    private final HttpClient http;
    private Admin admin;

    PipelineStatus(String bootstrap, HttpClient http) {
        this.bootstrap = bootstrap;
        this.http = http;
    }

    private synchronized Admin admin() {
        if (admin == null) {
            admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000, AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000,
                    AdminClientConfig.CLIENT_ID_CONFIG, "trace-ui"));
        }
        return admin;
    }

    /** {@code {groups: [{group, state, lag, topics: {topic: {committed, end, lag}}}], error?}} */
    Map<String, Object> lag() {
        Map<String, Object> out = new LinkedHashMap<>();
        if (bootstrap == null || bootstrap.isBlank()) {
            out.put("error", "KAFKA_BOOTSTRAP is not set: start with scripts/up.sh <store> --analytics");
            out.put("groups", List.of());
            return out;
        }
        try {
            Admin a = admin();
            List<String> groups = new ArrayList<>();
            for (GroupListing g : a.listGroups(ListGroupsOptions.forConsumerGroups()).all().get(5, TimeUnit.SECONDS)) {
                groups.add(g.groupId());
            }
            groups.sort(null);
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committed = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (String g : groups) {
                Map<TopicPartition, OffsetAndMetadata> offs = a.listConsumerGroupOffsets(g).partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                committed.put(g, offs);
                offs.keySet().forEach(tp -> latest.put(tp, OffsetSpec.latest()));
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = latest.isEmpty() ? Map.of()
                    : a.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
            Map<String, String> states = new HashMap<>();
            a.describeConsumerGroups(groups).all().get(5, TimeUnit.SECONDS)
                    .forEach((g, d) -> states.put(g, d.groupState().toString()));
            List<Map<String, Object>> rows = new ArrayList<>();
            for (String g : groups) {
                Map<String, long[]> perTopic = new TreeMap<>();
                committed.get(g).forEach((tp, om) -> {
                    if (om == null) {
                        return;
                    }
                    long end = ends.containsKey(tp) ? ends.get(tp).offset() : om.offset();
                    long[] acc = perTopic.computeIfAbsent(tp.topic(), t -> new long[3]);
                    acc[0] += om.offset();
                    acc[1] += end;
                    acc[2] += Math.max(0, end - om.offset());
                });
                Map<String, Object> topics = new LinkedHashMap<>();
                long total = 0;
                for (var e : perTopic.entrySet()) {
                    topics.put(e.getKey(), Map.of("committed", e.getValue()[0], "end", e.getValue()[1], "lag", e.getValue()[2]));
                    total += e.getValue()[2];
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("group", g);
                row.put("target", target(g));
                row.put("state", states.getOrDefault(g, "?"));
                row.put("lag", total);
                row.put("topics", topics);
                rows.add(row);
            }
            out.put("groups", rows);
        } catch (ExecutionException | TimeoutException e) {
            out.put("error", "Kafka not reachable at " + bootstrap + ": " + e.getMessage());
            out.put("groups", List.of());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.put("error", "interrupted");
            out.put("groups", List.of());
        }
        out.put("deephaven", "no consumer group: Deephaven re-reads from offset 0 on start and commits nothing");
        return out;
    }

    static String target(String group) {
        if (group.startsWith("sink-clickhouse")) {
            return "clickhouse";
        }
        if (group.startsWith("sink-duckdb")) {
            return "duckdb";
        }
        return "other";
    }

    /** True if {@code url} answers HTTP (any status) within a second. */
    boolean reachable(String url) {
        if (url == null || url.isBlank()) {
            return false;
        }
        try {
            HttpResponse<Void> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(1)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
            return r.statusCode() > 0;
        } catch (java.io.IOException | IllegalArgumentException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ---------------------------------------------------------------- dead-letter topic

    private KafkaConsumer<String, byte[]> dlqConsumer;
    private final Map<String, Long> dlqByTarget = new TreeMap<>();
    private String dlqLatestError;

    /**
     * Records in the dead-letter topic per {@code target} header (the consumer group that gave up), read
     * incrementally from the start of the topic (7 days of retention), plus the latest error.
     */
    synchronized Map<String, Object> deadLetters() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", DLQ_TOPIC);
        if (bootstrap == null || bootstrap.isBlank()) {
            out.put("byTarget", Map.of());
            return out;
        }
        try {
            if (dlqConsumer == null) {
                Properties p = new Properties();
                p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                p.put(ConsumerConfig.CLIENT_ID_CONFIG, "trace-ui-dlq");
                p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
                p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
                dlqConsumer = new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer());
            }
            List<TopicPartition> parts = dlqConsumer.partitionsFor(DLQ_TOPIC, Duration.ofSeconds(3)).stream()
                    .map(i -> new TopicPartition(i.topic(), i.partition())).toList();
            if (parts.isEmpty()) {
                out.put("byTarget", Map.of());
                out.put("error", "topic " + DLQ_TOPIC + " does not exist (kafka-init creates it)");
                return out;
            }
            if (!dlqConsumer.assignment().containsAll(parts)) {
                dlqConsumer.assign(parts);
                dlqConsumer.seekToBeginning(parts);
                dlqByTarget.clear();
            }
            Map<TopicPartition, Long> end = dlqConsumer.endOffsets(parts, Duration.ofSeconds(3));
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline
                    && parts.stream().anyMatch(tp -> dlqConsumer.position(tp, Duration.ofSeconds(3)) < end.get(tp))) {
                for (ConsumerRecord<String, byte[]> r : dlqConsumer.poll(Duration.ofMillis(200))) {
                    Header t = r.headers().lastHeader("target");
                    String target = t == null ? "unknown" : new String(t.value(), StandardCharsets.UTF_8);
                    dlqByTarget.merge(target, 1L, Long::sum);
                    Header err = r.headers().lastHeader("error");
                    if (err != null) {
                        dlqLatestError = target + ": " + new String(err.value(), StandardCharsets.UTF_8);
                    }
                }
            }
            out.put("byTarget", Map.copyOf(dlqByTarget));
            out.put("total", dlqByTarget.values().stream().mapToLong(Long::longValue).sum());
            out.put("latestError", dlqLatestError);
        } catch (KafkaException e) {
            out.put("byTarget", Map.copyOf(dlqByTarget));
            out.put("error", "reading " + DLQ_TOPIC + " failed: " + e.getMessage());
        }
        return out;
    }

    // ---------------------------------------------------------------- per-second rates

    private final Map<String, long[]> lastSample = new HashMap<>();

    /** Per-second rate of a monotonically increasing counter since the previous call ({@code null} at first). */
    synchronized Double rate(String key, long value) {
        long now = System.currentTimeMillis();
        long[] prev = lastSample.put(key, new long[] {now, value});
        if (prev == null || now <= prev[0] || value < prev[1]) {
            return null;
        }
        return Math.round((value - prev[1]) * 10_000.0 / (now - prev[0])) / 10.0;
    }

    @Override
    public synchronized void close() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(1));
        }
        if (dlqConsumer != null) {
            dlqConsumer.close(CloseOptions.timeout(Duration.ofSeconds(1)));
        }
    }
}
