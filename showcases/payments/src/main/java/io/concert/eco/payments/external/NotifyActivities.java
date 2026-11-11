package io.concert.eco.payments.external;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Publishes a payment message to Kafka, as a Temporal activity. Kafka has no idempotency keys across producer sessions:
 * a retry after a lost ack writes the record again. Every record therefore carries its idempotency key (record key plus
 * header {@code idempotency-key}) so consumers keep the first and drop repeats: the effect is once, delivery at least once.
 */
@ActivityInterface
public interface NotifyActivities {

    /** {@code type} is SETTLED or REVERSED (the compensation of a settlement whose charge was refunded). */
    record PublishRequest(String topic, String key, String idempotencyKey, String type, String json) {}

    /** Where the record landed, and the activity attempt whose send was acknowledged. */
    record Receipt(int partition, long offset, int attempt) {}

    @ActivityMethod(name = "PaymentsKafkaPublish")
    Receipt publish(PublishRequest request);
}
