package io.concert.trading.machines;

import io.concert.model.runtime.ModelObject;
import io.concert.sdk.ModelStateMachine;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.common.OrderType;
import io.concert.trading.common.Side;
import io.temporal.workflow.Workflow;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Objects;

/**
 * Base of the four trading machines. Their {@code @LegendModel} names the file set of
 * {@code trading-model}'s {@code TradingModels} (same {@code files} and {@code javaPackage}, own
 * {@code root}). The classes are generated in {@code trading-model}; this module runs no annotation
 * processor, so here the annotation only records the model and aggregate root of a machine.
 *
 * <p>Handlers maintain only their own aggregate; an entity never reads
 * another entity's data (each is its own workflow), so cross-entity consistency is checked from the
 * payload fields the producer repeats on every event ({@code orderId}, {@code accountId}, ...).
 *
 * <p>Timestamps are event time: the payload's {@code ts} (required by {@code OrderEvent}), falling back
 * to the workflow clock, so replay and re-delivery produce the same data.
 */
abstract class TradingStateMachine<D extends ModelObject> extends ModelStateMachine<D> {

    /** Scale of average prices (the analytics stores read at most 6 decimals). */
    static final int PX_SCALE = 6;

    /** Event time of a payload: its {@code ts}, else the (deterministic) workflow clock. */
    protected static Instant eventTime(OrderEvent p) {
        return p != null && p.getTs() != null ? p.getTs() : Instant.ofEpochMilli(Workflow.currentTimeMillis());
    }

    /** Rejects unless the payload field equals the entity's value (when both are known). */
    protected static void requireSame(String what, Object entity, Object payload) {
        if (entity != null && payload != null && !Objects.equals(entity, payload)) {
            reject(what + " mismatch: entity has " + entity + ", event has " + payload);
        }
    }

    protected static void requirePositive(String what, Long qty) {
        if (qty == null || qty <= 0) {
            reject(what + " must be positive, got " + qty);
        }
    }

    /**
     * Rejects a fill priced through the limit: a buy above it, a sell (or short sell) below it. Only
     * {@code LIMIT} orders with a limit price are checked.
     */
    protected static void checkLimit(OrderType type, Side side, BigDecimal limit, FillCommand fill) {
        if (type != OrderType.LIMIT || limit == null) {
            return;
        }
        int c = fill.getPrice().compareTo(limit);
        if (side == Side.BUY ? c > 0 : c < 0) {
            reject("limit violation: " + side + " fill " + fill.getFillId() + " at " + fill.getPrice().toPlainString()
                    + (side == Side.BUY ? " above" : " below") + " limit " + limit.toPlainString());
        }
    }

    /**
     * Checks a fill against a target quantity: no overfill, {@code complete_fill} must fill exactly, a
     * plain {@code fill} must leave quantity open (otherwise the producer must send
     * {@code complete_fill}, which is the transition into the done state).
     *
     * @return the new filled quantity
     */
    protected static long checkFill(String entity, long filled, long quantity, FillCommand fill, boolean complete) {
        requirePositive("fill quantity", fill.getQuantity());
        if (fill.getPrice() == null || fill.getPrice().signum() <= 0) {
            reject("fill price must be positive, got " + fill.getPrice());
        }
        long next = filled + fill.getQuantity();
        if (next > quantity) {
            reject("overfill: " + entity + " filled " + filled + " + " + fill.getQuantity() + " > quantity " + quantity);
        }
        if (complete && next != quantity) {
            reject("complete_fill leaves " + (quantity - next) + " of " + entity + " open");
        }
        if (!complete && next == quantity) {
            reject("fill completes " + entity + ": expected complete_fill");
        }
        return next;
    }

    /** {@code notional / qty} at {@link #PX_SCALE}, half-even. */
    protected static BigDecimal average(BigDecimal notional, long qty) {
        return notional.divide(BigDecimal.valueOf(qty), PX_SCALE, RoundingMode.HALF_EVEN);
    }
}
