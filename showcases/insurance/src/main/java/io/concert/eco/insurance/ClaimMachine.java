// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as ClaimMachine.java.bak-<timestamp>).
package io.concert.eco.insurance;

import io.concert.common.EventEnvelope;
import io.concert.eco.insurance.model.insurance.ApproveClaim;
import io.concert.eco.insurance.model.insurance.AssessClaim;
import io.concert.eco.insurance.model.insurance.Claim;
import io.concert.eco.insurance.model.insurance.ClaimStatus;
import io.concert.eco.insurance.model.insurance.FileClaim;
import io.concert.eco.insurance.model.insurance.PayClaim;
import io.concert.eco.insurance.model.insurance.RejectClaim;
import io.concert.eco.insurance.model.insurance.WithdrawClaim;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;
import io.temporal.workflow.Workflow;
import java.time.Instant;

/**
 * The {@code claim} state machine: entity data {@code insurance::Claim}, transitions
 * from {@link ClaimSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   NEW -file-&gt; OPEN : insurance::FileClaim
 *   OPEN -assess-&gt; ASSESSED : insurance::AssessClaim
 *   OPEN -withdraw-&gt; WITHDRAWN : insurance::WithdrawClaim
 *   ASSESSED -approve-&gt; APPROVED : insurance::ApproveClaim
 *   ASSESSED -reject-&gt; REJECTED : insurance::RejectClaim
 *   APPROVED -pay-&gt; PAID : insurance::PayClaim
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class ClaimMachine extends ModelStateMachine<Claim> {

    @Override
    protected StateMachineSpec spec() {
        return ClaimSpec.SPEC;
    }

    @Override
    protected Class<Claim> dataType() {
        return Claim.class;
    }

    @Override
    protected Claim initialData(String instanceKey) {
        return new Claim().setClaimId(instanceKey).setStatus(ClaimStatus.NEW);
    }

    @Override
    protected void onTransition(String from, String to, Claim claim, Object payload, EventEnvelope event) {
        claim.setStatus(ClaimStatus.valueOf(to));
        Instant now = Instant.ofEpochMilli(Workflow.currentTimeMillis());
        switch (payload) {
            case FileClaim cmd -> {
                if (cmd.getClaimed().getAmount().signum() <= 0) {
                    reject("claimed amount must be positive");
                }
                claim.setPolicyId(cmd.getPolicyId()).setClaimant(cmd.getClaimant()).setLossType(cmd.getLossType())
                        .setLossDate(cmd.getLossDate()).setDescription(cmd.getDescription()).setClaimed(cmd.getClaimed())
                        .setFiledAt(now);
            }
            case AssessClaim cmd -> {
                if (cmd.getAssessedAmount().compareTo(claim.getClaimed().getAmount()) > 0) {
                    reject("assessed " + cmd.getAssessedAmount().toPlainString() + " exceeds claimed "
                            + claim.getClaimed().getAmount().toPlainString());
                }
                claim.setAssessor(cmd.getAssessor()).setAssessedAmount(cmd.getAssessedAmount());
            }
            case WithdrawClaim cmd -> claim.setRejectReason("withdrawn: " + cmd.getReason()).setClosedAt(now);
            case ApproveClaim cmd -> {
                if (!cmd.getPolicyId().equals(claim.getPolicyId())) {
                    reject("approval for policy " + cmd.getPolicyId() + " but the claim is on " + claim.getPolicyId());
                }
                if (cmd.getApprovedAmount().compareTo(claim.getAssessedAmount()) > 0) {
                    reject("approved " + cmd.getApprovedAmount().toPlainString() + " exceeds assessed "
                            + claim.getAssessedAmount().toPlainString());
                }
                claim.setApprovedAmount(cmd.getApprovedAmount());
            }
            case RejectClaim cmd -> claim.setRejectReason(cmd.getReason()).setClosedAt(now);
            case PayClaim cmd -> {
                if (cmd.getPaidAmount().compareTo(claim.getApprovedAmount()) != 0) {
                    reject("payment " + cmd.getPaidAmount().toPlainString() + " differs from approved "
                            + claim.getApprovedAmount().toPlainString());
                }
                claim.setPayeeAccount(cmd.getPayeeAccount()).setPaidAmount(cmd.getPaidAmount()).setClosedAt(now);
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
