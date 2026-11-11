package io.concert.eco.payments.external;

import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory stand-ins for the ledger and Kafka, for tests: same idempotency semantics as the real services, plus
 * scripted faults (fail the first n calls after committing, or fail every publish).
 */
public final class InMemoryExternals {
    private InMemoryExternals() {}

    /** Ledger: one commit per idempotency key; charges over the limit are declined. */
    public static final class Ledger implements LedgerActivities {
        public final Map<String, Receipt> committed = new ConcurrentHashMap<>();
        public final List<String> refunded = new CopyOnWriteArrayList<>();
        public final AtomicInteger requests = new AtomicInteger();
        /** Calls that commit but then fail, as if the answer was lost. */
        public final AtomicInteger loseAnswers = new AtomicInteger();

        @Override
        public Receipt charge(ChargeRequest r) {
            requests.incrementAndGet();
            if (r.amount().compareTo(LedgerService.LIMIT) > 0) {
                throw ApplicationFailure.newNonRetryableFailure("insufficient funds", "Declined");
            }
            int attempt = Activity.getExecutionContext().getInfo().getAttempt();
            Receipt prior = committed.putIfAbsent(r.idempotencyKey(), new Receipt("LDG-" + r.paymentId(), "CHARGED", false, attempt));
            if (loseAnswers.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw ApplicationFailure.newFailure("committed, answer lost", "LedgerNoAnswer");
            }
            Receipt c = committed.get(r.idempotencyKey());
            return new Receipt(c.ledgerRef(), c.status(), prior != null, attempt);
        }

        @Override
        public Receipt refund(String idempotencyKey, String paymentId) {
            if (!refunded.contains(paymentId)) {
                refunded.add(paymentId);
            }
            return new Receipt("LDG-" + paymentId, "REFUNDED", false, Activity.getExecutionContext().getInfo().getAttempt());
        }
    }

    /** Kafka: an append-only list of records; may lose acks after writing or refuse every SETTLED publish. */
    public static final class Topic implements NotifyActivities {
        public final List<PublishRequest> records = new CopyOnWriteArrayList<>();
        public final AtomicInteger loseAcks = new AtomicInteger();
        public volatile boolean refuseSettled;

        @Override
        public Receipt publish(PublishRequest r) {
            if (refuseSettled && r.type().equals("SETTLED")) {
                throw ApplicationFailure.newFailure("broker unavailable", "KafkaNoAck");
            }
            records.add(r);
            if (loseAcks.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw ApplicationFailure.newFailure("written, ack lost", "KafkaAckLost");
            }
            return new Receipt(0, records.size() - 1, Activity.getExecutionContext().getInfo().getAttempt());
        }
    }
}
