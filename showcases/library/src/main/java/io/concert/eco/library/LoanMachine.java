// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as LoanMachine.java.bak-<timestamp>).
package io.concert.eco.library;

import io.concert.common.EventEnvelope;
import io.concert.eco.library.model.library.BorrowBook;
import io.concert.eco.library.model.library.CancelLoan;
import io.concert.eco.library.model.library.Loan;
import io.concert.eco.library.model.library.LoanStatus;
import io.concert.eco.library.model.library.RenewLoan;
import io.concert.eco.library.model.library.ReportLost;
import io.concert.eco.library.model.library.ReturnBook;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;
import io.temporal.workflow.Workflow;
import java.time.Instant;

/**
 * The {@code loan} state machine: entity data {@code library::Loan}, transitions
 * from {@link LoanSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   REQUESTED -borrow-&gt; BORROWED : library::BorrowBook
 *   REQUESTED -cancel-&gt; CANCELLED : library::CancelLoan
 *   BORROWED -renew-&gt; BORROWED : library::RenewLoan
 *   BORROWED -return-&gt; RETURNED : library::ReturnBook
 *   BORROWED -lose-&gt; LOST : library::ReportLost
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class LoanMachine extends ModelStateMachine<Loan> {

    @Override
    protected StateMachineSpec spec() {
        return LoanSpec.SPEC;
    }

    @Override
    protected Class<Loan> dataType() {
        return Loan.class;
    }

    @Override
    protected Loan initialData(String instanceKey) {
        return new Loan().setLoanId(instanceKey).setStatus(LoanStatus.REQUESTED);
    }

    /** A loan can be renewed this many times. */
    static final int MAX_RENEWALS = 2;

    @Override
    protected void onTransition(String from, String to, Loan loan, Object payload, EventEnvelope event) {
        loan.setStatus(LoanStatus.valueOf(to));
        Instant now = Instant.ofEpochMilli(Workflow.currentTimeMillis());
        switch (payload) {
            case BorrowBook cmd -> loan.setMemberId(cmd.getMemberId()).setBookId(cmd.getBookId())
                    .setDueDate(cmd.getDueDate()).setBorrowedAt(now);
            case RenewLoan cmd -> {
                if (loan.getRenewals() >= MAX_RENEWALS) {
                    reject("a loan can be renewed at most " + MAX_RENEWALS + " times");
                }
                if (!cmd.getDueDate().isAfter(loan.getDueDate())) {
                    reject("the new due date must be after " + loan.getDueDate());
                }
                loan.setDueDate(cmd.getDueDate()).setRenewals(loan.getRenewals() + 1);
            }
            case ReturnBook cmd -> loan.setNote(cmd.getCondition()).setClosedAt(now);
            case ReportLost cmd -> loan.setNote(cmd.getNote()).setClosedAt(now);
            case CancelLoan cmd -> loan.setNote(cmd.getReason()).setClosedAt(now);
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
