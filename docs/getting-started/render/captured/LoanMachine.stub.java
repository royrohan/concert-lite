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
 * from {@code now()}, the transition's timestamp (decisions may run more than once: no side effects).
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

    @Override
    protected void onTransition(String from, String to, Loan loan, Object payload, EventEnvelope event) {
        loan.setStatus(LoanStatus.valueOf(to));
        switch (payload) {
            case BorrowBook cmd -> {
                // TODO REQUESTED -borrow-> BORROWED: apply the command to the loan, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the loan
                if (cmd.getMemberId() != null) {
                    loan.setMemberId(cmd.getMemberId());
                }
                if (cmd.getBookId() != null) {
                    loan.setBookId(cmd.getBookId());
                }
                if (cmd.getDueDate() != null) {
                    loan.setDueDate(cmd.getDueDate());
                }
            }
            case CancelLoan cmd -> {
                // TODO REQUESTED -cancel-> CANCELLED: apply the command to the loan, call activities, or reject("...")
            }
            case RenewLoan cmd -> {
                // TODO BORROWED -renew-> BORROWED: apply the command to the loan, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the loan
                if (cmd.getDueDate() != null) {
                    loan.setDueDate(cmd.getDueDate());
                }
            }
            case ReturnBook cmd -> {
                // TODO BORROWED -return-> RETURNED: apply the command to the loan, call activities, or reject("...")
            }
            case ReportLost cmd -> {
                // TODO BORROWED -lose-> LOST: apply the command to the loan, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the loan
                if (cmd.getNote() != null) {
                    loan.setNote(cmd.getNote());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
