// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as SettlementMachine.java.bak-<timestamp>).
package io.concert.eco.payments;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.eco.payments.external.LedgerActivities;
import io.concert.eco.payments.external.NotifyActivities;
import io.concert.eco.payments.model.payments.CancelPayment;
import io.concert.eco.payments.model.payments.Payment;
import io.concert.eco.payments.model.payments.PaymentStatus;
import io.concert.eco.payments.model.payments.SettlePayment;
import io.concert.eco.payments.model.payments.SettlementOutcome;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Saga;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code settlement} state machine: entity data {@code payments::Payment}, transitions
 * from {@link SettlementSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   NEW -settle-&gt; SETTLING : payments::SettlePayment
 *   NEW -cancel-&gt; CANCELLED : payments::CancelPayment
 *   SETTLING -confirm-&gt; SETTLED : payments::SettlementOutcome
 *   SETTLING -decline-&gt; DECLINED : payments::SettlementOutcome
 *   SETTLING -fail-&gt; FAILED : payments::SettlementOutcome
 * </pre>
 *
 * <p><b>Settling against two external systems.</b> {@code settle} charges the ledger (REST) and then publishes the
 * settlement to Kafka, both as activities, and sends the outcome back to this payment as a chained event:
 * <ul>
 *   <li>Every write has an idempotency key derived from the payment id ({@code <id>:charge}, {@code <id>:settled}, ...),
 *       the same on every retry, worker crash and replay.
 *   <li>A failed attempt (500, timeout, dropped connection, lost Kafka ack) is retried by Temporal; the outcome of the
 *       failed attempt is unknown, and the retry finds out: the ledger answers a known key with the stored result,
 *       Kafka consumers drop repeats of a key.
 *   <li>Once an activity has completed, its result is in the workflow history: a crash or replay after that point
 *       never calls the service again.
 *   <li>A refusal (the ledger's 422) is final: no retry, the payment is declined.
 *   <li>When a step cannot be completed in time, a {@link Saga} undoes what may have happened. Compensations are
 *       registered <em>before</em> each ambiguous call (a charge that timed out may still have committed): refund the
 *       charge, and publish a REVERSED record for a settlement record that may have been written. Compensations retry
 *       until they succeed.
 * </ul>
 */
public class SettlementMachine extends ModelStateMachine<Payment> {

    /** The topic settlements (and their reversals) are published to. */
    public static final String TOPIC = "payments.settled";

    /** Charges: a hung call is abandoned after 5 s and retried, for up to 2 minutes in all. */
    private final LedgerActivities ledger = Workflow.newActivityStub(LedgerActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .setScheduleToCloseTimeout(Duration.ofMinutes(2))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofMillis(500))
                    .setBackoffCoefficient(2)
                    .setMaximumInterval(Duration.ofSeconds(5))
                    .build())
            .build());

    /** Publishes: 12 s per attempt (the producer gives up after 8 s), 40 s in all; then the settlement is undone. */
    private final NotifyActivities kafka = Workflow.newActivityStub(NotifyActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(12))
            .setScheduleToCloseTimeout(Duration.ofSeconds(40))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(1))
                    .setMaximumInterval(Duration.ofSeconds(5))
                    .build())
            .build());

    /** Compensations must happen: unlimited retries. */
    private final ActivityOptions mustSucceed = ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(12))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(1))
                    .setMaximumInterval(Duration.ofSeconds(10))
                    .setMaximumAttempts(0)
                    .build())
            .build();
    private final LedgerActivities refunds = Workflow.newActivityStub(LedgerActivities.class, mustSucceed);
    private final NotifyActivities reversals = Workflow.newActivityStub(NotifyActivities.class, mustSucceed);

    @Override
    protected StateMachineSpec spec() {
        return SettlementSpec.SPEC;
    }

    @Override
    protected Class<Payment> dataType() {
        return Payment.class;
    }

    @Override
    protected Payment initialData(String instanceKey) {
        return new Payment().setPaymentId(instanceKey).setStatus(PaymentStatus.NEW);
    }

    @Override
    protected void onTransition(String from, String to, Payment payment, Object payload, EventEnvelope event) {
        payment.setStatus(PaymentStatus.valueOf(to));
        switch (payload) {
            case SettlePayment cmd -> {
                payment.setAccountId(cmd.getAccountId()).setAmount(cmd.getAmount()).setCurrency(cmd.getCurrency())
                        .setMemo(cmd.getMemo());
                settle(payment);
            }
            case SettlementOutcome o -> {
                payment.setLedgerRef(o.getLedgerRef()).setLedgerReplayed(o.getLedgerReplayed())
                        .setKafkaPartition(o.getKafkaPartition()).setKafkaOffset(o.getKafkaOffset())
                        .setReason(o.getReason()).setRefunded(Boolean.TRUE.equals(o.getRefunded()));
                if (o.getLedgerAttempts() != null) {
                    payment.setLedgerAttempts(o.getLedgerAttempts());
                }
                if (o.getKafkaAttempts() != null) {
                    payment.setKafkaAttempts(o.getKafkaAttempts());
                }
                if (to.equals("SETTLED")) {
                    payment.setSettledAt(Instant.ofEpochMilli(Workflow.currentTimeMillis()));
                }
            }
            case CancelPayment cmd -> payment.setReason(cmd.getReason());
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }

    /** Charge, publish, and send the outcome back to this payment (confirm, decline or fail). */
    private void settle(Payment payment) {
        String id = payment.getPaymentId();
        SettlementOutcome out = new SettlementOutcome();
        Saga saga = new Saga(new Saga.Options.Builder().build());
        String outcome;
        try {
            // a charge that failed ambiguously may have committed: register its refund first (a refund of nothing is a no-op)
            saga.addCompensation(() -> refunds.refund(id + ":refund", id));
            LedgerActivities.Receipt charge = ledger.charge(
                    new LedgerActivities.ChargeRequest(id + ":charge", id, payment.getAccountId(), payment.getAmount(), payment.getCurrency()));
            out.setLedgerRef(charge.ledgerRef()).setLedgerAttempts((long) charge.attempt()).setLedgerReplayed(charge.replayed());

            // a publish that failed ambiguously may have written the record: register its reversal first
            saga.addCompensation(() -> reversals.publish(record(payment, charge.ledgerRef(), "REVERSED")));
            NotifyActivities.Receipt sent = kafka.publish(record(payment, charge.ledgerRef(), "SETTLED"));
            out.setKafkaPartition((long) sent.partition()).setKafkaOffset(sent.offset()).setKafkaAttempts((long) sent.attempt());
            outcome = "confirm";
        } catch (ActivityFailure f) {
            if (declined(f)) {
                // a refusal is final and nothing was committed: no compensation
                out.setReason("declined by the ledger: " + message(f));
                outcome = "decline";
            } else {
                saga.compensate();
                out.setReason("not settled, undone (charge refunded, settlement reversed): " + message(f)).setRefunded(true);
                outcome = "fail";
            }
        }
        // the outcome arrives as this payment's next event (id <event id>.1, so retries and replays cannot repeat it)
        emitEntity("settlement", id, outcome, out, List.of());
    }

    /** The Kafka record: key = payment id, idempotency key = payment id + type. Built here, so it is deterministic. */
    private static NotifyActivities.PublishRequest record(Payment p, String ledgerRef, String type) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("paymentId", p.getPaymentId());
        m.put("accountId", p.getAccountId());
        m.put("amount", p.getAmount());
        m.put("currency", p.getCurrency());
        m.put("ledgerRef", ledgerRef);
        m.put("memo", p.getMemo());
        return new NotifyActivities.PublishRequest(TOPIC, p.getPaymentId(), p.getPaymentId() + ":" + type.toLowerCase(),
                type, Json.write(m));
    }

    private static boolean declined(ActivityFailure f) {
        return f.getCause() instanceof ApplicationFailure a && "Declined".equals(a.getType());
    }

    /** The innermost failure's message: what the service or the timeout said. */
    private static String message(TemporalFailure f) {
        Throwable t = f;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t instanceof ApplicationFailure a ? a.getOriginalMessage() : String.valueOf(t.getMessage());
    }
}
