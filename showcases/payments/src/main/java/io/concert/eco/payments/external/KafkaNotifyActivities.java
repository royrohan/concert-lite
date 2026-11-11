package io.concert.eco.payments.external;

import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * {@link NotifyActivities} with a Kafka producer ({@code KAFKA_BOOTSTRAP}).
 *
 * <p>The producer is idempotent ({@code enable.idempotence}, {@code acks=all}): the broker drops repeats of the
 * producer's own internal retries. A failed activity attempt, however, is retried by Temporal with a new send, which
 * Kafka cannot tell from a new record, so the record may be written twice. Consumers deduplicate on the
 * {@code idempotency-key} header (the record key works too: one settlement per payment).
 *
 * <p>Chaos: {@link ChaosControl#kafkaAckLossRate()} drops the acknowledgement of that share of sends after the broker
 * has written the record, exactly the "published, but no commit came back" case.
 */
public final class KafkaNotifyActivities implements NotifyActivities, AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final String bootstrap;
    private final ChaosControl chaos;
    private final java.util.Set<String> topics = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public KafkaNotifyActivities(String bootstrap, String ledgerUrl) {
        this.bootstrap = bootstrap;
        this.chaos = new ChaosControl(ledgerUrl);
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // a send gives up well inside the activity's 12 s start-to-close, so Temporal sees the failure and retries
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8000);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 4000);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(p);
    }

    @Override
    public Receipt publish(PublishRequest r) {
        int attempt = Activity.getExecutionContext().getInfo().getAttempt();
        ensureTopic(r.topic());
        ProducerRecord<String, String> record = new ProducerRecord<>(r.topic(), r.key(), r.json());
        record.headers().add("idempotency-key", r.idempotencyKey().getBytes(StandardCharsets.UTF_8));
        record.headers().add("type", r.type().getBytes(StandardCharsets.UTF_8));
        record.headers().add("attempt", Integer.toString(attempt).getBytes(StandardCharsets.UTF_8));
        RecordMetadata md;
        try {
            md = producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            // not acknowledged: the record may or may not be written; Temporal retries, consumers deduplicate
            throw ApplicationFailure.newFailure("kafka send not acknowledged (attempt " + attempt + "): " + e, "KafkaNoAck");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ApplicationFailure.newFailure("interrupted", "KafkaNoAck");
        }
        if (ThreadLocalRandom.current().nextDouble() < chaos.kafkaAckLossRate()) {
            throw ApplicationFailure.newFailure("chaos: ack dropped after the record was written at " + md.topic() + "-"
                    + md.partition() + "@" + md.offset() + " (attempt " + attempt + ")", "KafkaAckLost");
        }
        return new Receipt(md.partition(), md.offset(), attempt);
    }

    private void ensureTopic(String topic) {
        if (topics.contains(topic)) {
            return;
        }
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000, AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw ApplicationFailure.newFailure("cannot create topic " + topic + ": " + e.getCause(), "KafkaNoAck");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw ApplicationFailure.newFailure("cannot create topic " + topic + ": " + e, "KafkaNoAck");
        }
        topics.add(topic);
    }

    @Override
    public void close() {
        producer.close();
    }
}
