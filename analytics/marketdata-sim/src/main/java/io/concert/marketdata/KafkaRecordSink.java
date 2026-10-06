package io.concert.marketdata;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Idempotent producer ({@code acks=all}, zstd): a retried send never duplicates or reorders records
 * within a partition, and per-symbol order holds because the key is the symbol. A failed async send
 * is rethrown on the next {@link #send} or {@link #flush}.
 */
public final class KafkaRecordSink implements RecordSink {

    private final KafkaProducer<String, String> producer;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public KafkaRecordSink(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "marketdata-sim");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(p);
    }

    /** Creates the topics that do not exist yet; existing topics are left as they are. */
    public static void ensureTopics(String bootstrap, List<MdTopics.TopicSpec> specs, short replication)
            throws InterruptedException, ExecutionException {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(p)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = specs.stream().filter(s -> !existing.contains(s.name()))
                    .map(s -> new NewTopic(s.name(), s.partitions(), replication).configs(s.configs())).toList();
            if (missing.isEmpty()) {
                return;
            }
            try {
                admin.createTopics(missing).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e; // a concurrent creator winning the race is fine
                }
            }
        }
    }

    @Override
    public void send(String topic, String key, String value) {
        rethrow();
        producer.send(new ProducerRecord<>(topic, key, value), (md, e) -> {
            if (e != null) {
                failure.compareAndSet(null, e);
            }
        });
    }

    @Override
    public void flush() {
        producer.flush();
        rethrow();
    }

    @Override
    public void close() {
        producer.close();
    }

    private void rethrow() {
        Exception e = failure.get();
        if (e != null) {
            throw new IllegalStateException("Kafka send failed", e);
        }
    }
}
