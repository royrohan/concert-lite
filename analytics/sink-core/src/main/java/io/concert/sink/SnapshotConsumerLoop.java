package io.concert.sink;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Feeds one {@link SinkTarget} from the snapshot topic: poll, decode, {@link SinkTarget#apply apply},
 * commit.
 *
 * <ul>
 *   <li>One consumer group per target ({@link Config#groupId}), auto-commit off.
 *   <li>If the target {@linkplain SinkTarget#storesOffsets() stores offsets}, every assigned partition
 *       is positioned from {@link SinkTarget#storedOffsets()} (from the beginning if it has none), so
 *       the target's own transaction is the checkpoint and the Kafka commit is informational
 *       (it shows lag in tools). The next offsets of <i>everything</i> polled, including skipped and
 *       dead-lettered records, go to the target with the batch, so they advance in the same transaction.
 *   <li>When {@code apply} fails with an ordinary exception (database down), the assignment is paused
 *       (the consumer keeps polling, so it stays in the group) and the same batch is retried with
 *       exponential backoff until it succeeds. A rebalance drops the pending batch: its partitions are
 *       re-read from the stored offsets.
 *   <li><b>Dead-letter topic</b> ({@link Config#dlqTopic}, default {@value #DEFAULT_DLQ}): a record that
 *       is not a snapshot (undecodable: deterministic, so it is not retried) or that the target rejects
 *       {@link Config#maxAttempts} times with a {@link SinkTarget.RecordException} (with backoff between
 *       attempts) is written there, synchronously and before the offsets advance, with its key, value and
 *       headers plus headers {@code error}, {@code target}, {@code attempts}, {@code original.topic},
 *       {@code original.partition} and {@code original.offset}; the loop then moves on. Delivery to the
 *       DLQ is at least once (a crash between the DLQ write and the offset commit writes it again). If the
 *       DLQ cannot be written (topic missing, Kafka down) the batch is retried like any transient failure,
 *       so nothing is skipped silently.
 * </ul>
 *
 * {@link #close()} stops the loop gracefully: the batch in progress finishes, then the consumer leaves
 * the group.
 */
public final class SnapshotConsumerLoop implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SnapshotConsumerLoop.class);

    public static final String DEFAULT_DLQ = SnapshotCodec.TOPIC + ".dlq";
    public static final String H_ERROR = "error";
    public static final String H_TARGET = "target";
    public static final String H_ATTEMPTS = "attempts";
    public static final String H_ORIGINAL_TOPIC = "original.topic";
    public static final String H_ORIGINAL_PARTITION = "original.partition";
    public static final String H_ORIGINAL_OFFSET = "original.offset";

    /**
     * @param overrides extra consumer properties (e.g. {@code max.poll.records})
     * @param dlqTopic dead-letter topic, or {@code null} to only log and count bad records (tests)
     * @param maxAttempts attempts before a record the target rejects goes to the DLQ
     */
    public record Config(String bootstrap, String topic, String groupId, Duration pollTimeout, Duration maxBackoff,
            Map<String, Object> overrides, String dlqTopic, int maxAttempts) {

        public Config(String bootstrap, String topic, String groupId, Duration pollTimeout, Duration maxBackoff,
                Map<String, Object> overrides) {
            this(bootstrap, topic, groupId, pollTimeout, maxBackoff, overrides, DEFAULT_DLQ, 3);
        }

        public Config(String bootstrap, String topic, String groupId) {
            this(bootstrap, topic, groupId, Duration.ofMillis(500), Duration.ofSeconds(30), Map.of());
        }
    }

    /** Counters for health endpoints. {@code lag}: records behind the end of the assigned partitions. */
    public record Stats(long batches, long records, long undecodable, long failures, long dlq, long lag, String lastError) {}

    /** A record to dead-letter, with why. */
    private record DeadLetter(ConsumerRecord<String, byte[]> raw, String error, int attempts) {}

    private final Config config;
    private final SinkTarget target;
    private final SinkMetrics metrics = new SinkMetrics();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong records = new AtomicLong();
    private final AtomicLong undecodable = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong dlq = new AtomicLong();
    private volatile long lag;
    private volatile String lastError;
    private volatile boolean running = true;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile Thread thread;
    private KafkaProducer<String, byte[]> dlqProducer;

    /** The batch being applied (retried until it succeeds); dropped when partitions are revoked. */
    private List<SnapshotRecord> pending;
    /** Raw records of the pending batch by position, for dead-lettering. */
    private Map<TopicPartition, Map<Long, ConsumerRecord<String, byte[]>>> pendingRaw;
    /** Next offset per partition of everything polled for the pending batch. */
    private Map<TopicPartition, Long> pendingOffsets;
    /** Records of the pending batch still to be written to the DLQ. */
    private List<DeadLetter> pendingDlq;
    /** Attempts per record (partition, offset) that the target rejected. */
    private final Map<TopicPartition, Map<Long, Integer>> attempts = new HashMap<>();

    public SnapshotConsumerLoop(Config config, SinkTarget target) {
        this.config = config;
        this.target = target;
    }

    /** Runs the loop on a new thread. */
    public Thread start() {
        Thread t = Thread.ofPlatform().name("sink-" + config.groupId()).start(this);
        thread = t;
        return t;
    }

    public Stats stats() {
        return new Stats(batches.get(), records.get(), undecodable.get(), failures.get(), dlq.get(), lag, lastError);
    }

    public SinkMetrics metrics() {
        return metrics;
    }

    public Config config() {
        return config;
    }

    @Override
    public void run() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, config.groupId() + "-" + ProcessHandle.current().pid());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        p.putAll(config.overrides());
        KafkaConsumer<String, byte[]> created = null;
        for (long wait = 1000; running && created == null; wait = Math.min(wait * 2, config.maxBackoff().toMillis())) {
            try {
                created = new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer());
            } catch (KafkaException e) {
                // e.g. the bootstrap host does not resolve yet (broker container still starting)
                lastError = e.toString();
                log.warn("{}: cannot create the Kafka consumer, retrying in {} ms: {}", config.groupId(), wait, e.toString());
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        if (created == null) {
            return;
        }
        try (KafkaConsumer<String, byte[]> c = created) {
            consumer = c;
            if (!running) {
                return;
            }
            c.subscribe(List.of(config.topic()), new Rebalance(c));
            long backoffMs = 0;
            while (running) {
                if (pending == null) {
                    ConsumerRecords<String, byte[]> polled = c.poll(config.pollTimeout());
                    updateLag(c);
                    if (polled.isEmpty()) {
                        continue;
                    }
                    decode(polled);
                }
                try {
                    flushDeadLetters();
                    long t0 = System.nanoTime();
                    if (target.storesOffsets() || !pending.isEmpty()) {
                        target.apply(pending, pendingOffsets);
                    }
                    metrics.recordBatch(System.nanoTime() - t0);
                    batches.incrementAndGet();
                    records.addAndGet(pending.size());
                    commit(c);
                    clearPending();
                    if (backoffMs > 0) {
                        c.resume(c.paused());
                        log.info("{}: target recovered", config.groupId());
                        backoffMs = 0;
                    }
                } catch (SinkTarget.RecordException e) {
                    if (!rejected(e)) {
                        failures.incrementAndGet();
                        long wait = Math.min(200L << Math.min(10, attemptsOf(e.record()) - 1), config.maxBackoff().toMillis());
                        c.pause(c.assignment());
                        pausedWait(c, wait);
                        c.resume(c.paused());
                    }
                } catch (RuntimeException e) {
                    if (e instanceof WakeupException w) {
                        throw w;
                    }
                    failures.incrementAndGet();
                    lastError = e.toString();
                    backoffMs = backoffMs == 0 ? 500 : Math.min(backoffMs * 2, config.maxBackoff().toMillis());
                    log.warn("{}: applying {} snapshots failed, retrying in {} ms: {}", config.groupId(),
                            pending == null ? 0 : pending.size(), backoffMs, e.toString());
                    c.pause(c.assignment());
                    pausedWait(c, backoffMs);
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } finally {
            consumer = null;
            if (dlqProducer != null) {
                dlqProducer.close(Duration.ofSeconds(5));
            }
            log.info("{}: consumer loop stopped", config.groupId());
        }
    }

    /** Polls while paused (returns nothing but keeps the group membership, and may rebalance). */
    private void pausedWait(KafkaConsumer<String, byte[]> c, long millis) {
        long until = System.currentTimeMillis() + millis;
        while (running && pending != null && System.currentTimeMillis() < until) {
            c.poll(Duration.ofMillis(Math.min(200, Math.max(1, until - System.currentTimeMillis()))));
        }
    }

    /**
     * Counts an attempt of the rejected record; at {@link Config#maxAttempts} moves it from the batch to the
     * dead letters and returns true (retry at once), else false (back off, then retry the batch).
     */
    private boolean rejected(SinkTarget.RecordException e) {
        SnapshotRecord r = e.record();
        int n = attempts.computeIfAbsent(r.topicPartition(), tp -> new HashMap<>()).merge(r.offset(), 1, Integer::sum);
        String why = e.getMessage() + (e.getCause() == null ? "" : ": " + e.getCause());
        lastError = why;
        if (n < config.maxAttempts() || pending == null || !pending.remove(r)) {
            log.warn("{}: record {}-{}@{} rejected (attempt {}/{}): {}", config.groupId(), r.topic(), r.partition(),
                    r.offset(), n, config.maxAttempts(), why);
            return false;
        }
        attempts.get(r.topicPartition()).remove(r.offset());
        ConsumerRecord<String, byte[]> raw = pendingRaw.getOrDefault(r.topicPartition(), Map.of()).get(r.offset());
        pendingDlq.add(new DeadLetter(raw, why, n));
        return true;
    }

    private int attemptsOf(SnapshotRecord r) {
        return attempts.getOrDefault(r.topicPartition(), Map.of()).getOrDefault(r.offset(), 1);
    }

    private void updateLag(KafkaConsumer<String, byte[]> c) {
        long total = 0;
        for (TopicPartition tp : c.assignment()) {
            total += c.currentLag(tp).orElse(0);
        }
        lag = total;
    }

    private void decode(ConsumerRecords<String, byte[]> polled) {
        List<SnapshotRecord> batch = new ArrayList<>(polled.count());
        Map<TopicPartition, Long> next = new HashMap<>();
        Map<TopicPartition, Map<Long, ConsumerRecord<String, byte[]>>> raw = new HashMap<>();
        List<DeadLetter> dead = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> r : polled) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            next.merge(tp, r.offset() + 1, Math::max);
            if (r.value() == null) {
                continue; // tombstone
            }
            try {
                batch.add(new SnapshotRecord(SnapshotCodec.decode(r.value()), r.topic(), r.partition(), r.offset()));
                raw.computeIfAbsent(tp, k -> new LinkedHashMap<>()).put(r.offset(), r);
            } catch (RuntimeException e) {
                undecodable.incrementAndGet();
                lastError = "undecodable " + r.topic() + "-" + r.partition() + "@" + r.offset() + ": " + e.getMessage();
                log.warn("{}: undecodable record {}-{}@{} (key {}){}: {}", config.groupId(), r.topic(), r.partition(),
                        r.offset(), r.key(), config.dlqTopic() == null ? ", skipped" : ", to the DLQ", e.getMessage());
                dead.add(new DeadLetter(r, "undecodable: " + e.getMessage(), 1));
            }
        }
        pending = batch;
        pendingRaw = raw;
        pendingOffsets = next;
        pendingDlq = dead;
    }

    /** Writes the pending dead letters (synchronously, so they are in Kafka before the offsets advance). */
    private void flushDeadLetters() {
        if (pendingDlq.isEmpty()) {
            return;
        }
        if (config.dlqTopic() == null) {
            pendingDlq.clear();
            return;
        }
        while (!pendingDlq.isEmpty()) {
            DeadLetter d = pendingDlq.getFirst();
            ProducerRecord<String, byte[]> out = new ProducerRecord<>(config.dlqTopic(), d.raw().key(), d.raw().value());
            for (Header h : d.raw().headers()) {
                out.headers().add(h);
            }
            String error = d.error().replaceAll("\\s*\\R\\s*", " ").strip();
            header(out, H_ERROR, error.length() > 2000 ? error.substring(0, 2000) : error);
            header(out, H_TARGET, config.groupId());
            header(out, H_ATTEMPTS, Integer.toString(d.attempts()));
            header(out, H_ORIGINAL_TOPIC, d.raw().topic());
            header(out, H_ORIGINAL_PARTITION, Integer.toString(d.raw().partition()));
            header(out, H_ORIGINAL_OFFSET, Long.toString(d.raw().offset()));
            try {
                dlqProducer().send(out).get(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while writing to " + config.dlqTopic(), e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IllegalStateException("writing to dead-letter topic " + config.dlqTopic() + " failed: "
                        + (e.getCause() == null ? e : e.getCause()), e);
            }
            pendingDlq.removeFirst();
            dlq.incrementAndGet();
            log.warn("{}: {}-{}@{} sent to {} after {} attempt(s): {}", config.groupId(), d.raw().topic(),
                    d.raw().partition(), d.raw().offset(), config.dlqTopic(), d.attempts(), d.error());
        }
    }

    private static void header(ProducerRecord<String, byte[]> r, String name, String value) {
        r.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private KafkaProducer<String, byte[]> dlqProducer() {
        if (dlqProducer == null) {
            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrap());
            p.put(ProducerConfig.CLIENT_ID_CONFIG, config.groupId() + "-dlq-" + ProcessHandle.current().pid());
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 8 * 1024 * 1024);
            p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
            p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
            dlqProducer = new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
        }
        return dlqProducer;
    }

    private void clearPending() {
        pending = null;
        pendingRaw = null;
        pendingOffsets = null;
        pendingDlq = null;
        attempts.clear();
    }

    private void commit(KafkaConsumer<String, byte[]> c) {
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        pendingOffsets.forEach((tp, next) -> commit.put(tp, new OffsetAndMetadata(next)));
        try {
            c.commitSync(commit);
        } catch (KafkaException e) {
            if (e instanceof WakeupException w) {
                throw w;
            }
            // The target's stored offsets are authoritative; a lost commit only affects lag metrics.
            log.debug("{}: offset commit failed: {}", config.groupId(), e.toString());
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        private final KafkaConsumer<String, byte[]> c;

        Rebalance(KafkaConsumer<String, byte[]> c) {
            this.c = c;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (pending != null && !partitions.isEmpty()) {
                log.info("{}: partitions revoked, dropping the pending batch of {}", config.groupId(), pending.size());
                clearPending();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty() || !target.storesOffsets()) {
                return;
            }
            Map<TopicPartition, Long> stored = target.storedOffsets();
            List<TopicPartition> fromStart = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                Long next = stored.get(tp);
                if (next == null) {
                    fromStart.add(tp);
                } else {
                    c.seek(tp, next);
                }
            }
            if (!fromStart.isEmpty()) {
                c.seekToBeginning(fromStart);
            }
            log.info("{}: assigned {} (stored offsets {})", config.groupId(), partitions, stored);
        }
    }

    @Override
    public void close() {
        running = false;
        KafkaConsumer<String, byte[]> c = consumer;
        Thread t = thread;
        if (c != null) {
            c.wakeup();
        } else if (t != null && t != Thread.currentThread()) {
            t.interrupt(); // still waiting to create the consumer
        }
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(Duration.ofSeconds(15));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
