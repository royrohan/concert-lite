// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as PolicyMachine.java.bak-<timestamp>).
package io.concert.eco.insurance;

import io.concert.common.EventEnvelope;
import io.concert.eco.insurance.model.insurance.BindPolicy;
import io.concert.eco.insurance.model.insurance.CancelPolicy;
import io.concert.eco.insurance.model.insurance.DeclineQuote;
import io.concert.eco.insurance.model.insurance.EndorsePolicy;
import io.concert.eco.insurance.model.insurance.ExpirePolicy;
import io.concert.eco.insurance.model.insurance.Policy;
import io.concert.eco.insurance.model.insurance.PolicyStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;
import io.temporal.workflow.Workflow;
import java.time.Instant;

/**
 * The {@code policy} state machine: entity data {@code insurance::Policy}, transitions
 * from {@link PolicySpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   QUOTED -bind-&gt; ACTIVE : insurance::BindPolicy
 *   QUOTED -decline-&gt; DECLINED : insurance::DeclineQuote
 *   ACTIVE -endorse-&gt; ACTIVE : insurance::EndorsePolicy
 *   ACTIVE -expire-&gt; EXPIRED : insurance::ExpirePolicy
 *   ACTIVE -cancel-&gt; CANCELLED : insurance::CancelPolicy
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class PolicyMachine extends ModelStateMachine<Policy> {

    @Override
    protected StateMachineSpec spec() {
        return PolicySpec.SPEC;
    }

    @Override
    protected Class<Policy> dataType() {
        return Policy.class;
    }

    @Override
    protected Policy initialData(String instanceKey) {
        return new Policy().setPolicyId(instanceKey).setStatus(PolicyStatus.QUOTED);
    }

    @Override
    protected void onTransition(String from, String to, Policy policy, Object payload, EventEnvelope event) {
        policy.setStatus(PolicyStatus.valueOf(to));
        Instant now = Instant.ofEpochMilli(Workflow.currentTimeMillis());
        switch (payload) {
            case BindPolicy cmd -> {
                if (cmd.getEndDate().isBefore(cmd.getStartDate())) {
                    reject("policy ends before it starts");
                }
                policy.setHolderName(cmd.getHolderName()).setProduct(cmd.getProduct()).setPremium(cmd.getPremium())
                        .setCoverageLimit(cmd.getCoverageLimit()).setDeductible(cmd.getDeductible())
                        .setStartDate(cmd.getStartDate()).setEndDate(cmd.getEndDate()).setBoundAt(now);
            }
            case DeclineQuote cmd -> policy.setCloseReason(cmd.getReason()).setClosedAt(now);
            case EndorsePolicy cmd -> policy.setCoverageLimit(cmd.getCoverageLimit()).setEndorsements(policy.getEndorsements() + 1);
            case ExpirePolicy cmd -> policy.setEndDate(cmd.getEndDate()).setCloseReason("expired").setClosedAt(now);
            case CancelPolicy cmd -> policy.setCloseReason(cmd.getReason()).setClosedAt(now);
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
