package io.concert.eco.payments.external;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.math.BigDecimal;

/**
 * The ledger REST service (charges and refunds), called as Temporal activities. Every request carries an
 * idempotency key: the ledger commits a key once and answers a repeat with the stored result, so a retry after an
 * ambiguous failure (committed, but the answer was lost) is safe and doubles as the status check.
 */
@ActivityInterface
public interface LedgerActivities {

    /** A charge request; {@code idempotencyKey} must be the same on every attempt of the same charge. */
    record ChargeRequest(String idempotencyKey, String paymentId, String accountId, BigDecimal amount, String currency) {}

    /** The ledger's answer: its reference, status, whether it was a stored repeat, and the attempt that got it. */
    record Receipt(String ledgerRef, String status, boolean replayed, int attempt) {}

    /** Charges the account. Fails non-retryably with type {@code Declined} when the ledger refuses the charge. */
    @ActivityMethod(name = "LedgerCharge")
    Receipt charge(ChargeRequest request);

    /** Refunds the payment's charge, if there is one (a refund of nothing succeeds: compensation is safe to run). */
    @ActivityMethod(name = "LedgerRefund")
    Receipt refund(String idempotencyKey, String paymentId);
}
