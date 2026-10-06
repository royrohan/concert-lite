// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as PayoutMachine.java.bak-<timestamp>).
package io.concert.eco.insurance;

import io.concert.common.EventEnvelope;
import io.concert.eco.insurance.model.insurance.DisbursePayout;
import io.concert.eco.insurance.model.insurance.FailPayout;
import io.concert.eco.insurance.model.insurance.Payout;
import io.concert.eco.insurance.model.insurance.PayoutStatus;
import io.concert.eco.insurance.model.insurance.SchedulePayout;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;
import io.temporal.workflow.Workflow;
import java.time.Instant;

/**
 * The {@code payout} state machine: entity data {@code insurance::Payout}, transitions
 * from {@link PayoutSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING -schedule-&gt; SCHEDULED : insurance::SchedulePayout
 *   SCHEDULED -disburse-&gt; DISBURSED : insurance::DisbursePayout
 *   SCHEDULED -fail-&gt; FAILED : insurance::FailPayout
 *   FAILED -retry-&gt; SCHEDULED : insurance::SchedulePayout
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class PayoutMachine extends ModelStateMachine<Payout> {

    @Override
    protected StateMachineSpec spec() {
        return PayoutSpec.SPEC;
    }

    @Override
    protected Class<Payout> dataType() {
        return Payout.class;
    }

    @Override
    protected Payout initialData(String instanceKey) {
        return new Payout().setPayoutId(instanceKey).setStatus(PayoutStatus.PENDING);
    }

    @Override
    protected void onTransition(String from, String to, Payout payout, Object payload, EventEnvelope event) {
        payout.setStatus(PayoutStatus.valueOf(to));
        Instant now = Instant.ofEpochMilli(Workflow.currentTimeMillis());
        switch (payload) {
            case SchedulePayout cmd -> {
                if (payout.getClaimId() != null && !payout.getClaimId().equals(cmd.getClaimId())) {
                    reject("payout belongs to claim " + payout.getClaimId());
                }
                payout.setClaimId(cmd.getClaimId()).setPayeeAccount(cmd.getPayeeAccount()).setMethod(cmd.getMethod())
                        .setAmount(cmd.getAmount()).setAttempts(payout.getAttempts() + 1).setScheduledAt(now);
            }
            case DisbursePayout cmd -> payout.setReference(cmd.getReference()).setDisbursedAt(now);
            case FailPayout cmd -> payout.setFailureReason(cmd.getReason());
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
