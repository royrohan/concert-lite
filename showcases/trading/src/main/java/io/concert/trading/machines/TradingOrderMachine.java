package io.concert.trading.machines;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.AckCommand;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.CancelCommand;
import io.concert.trading.command.CloseOrderCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.NewOrderCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.command.RejectCommand;
import io.concert.trading.command.RouteCommand;
import io.concert.trading.common.OrderType;
import io.concert.trading.order.Order;
import io.concert.trading.order.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * {@code trading_order}: the parent client order. Its data keeps <b>aggregates only</b> (filled,
 * routed, allocated quantities, average price, notional, fees, timestamps); the generated
 * {@code executions} / {@code allocations} association lists stay empty. Executions, fills and
 * allocations are entities of their own machines with their own terminal snapshots, joined to the
 * order by {@code orderId} in analytics.
 *
 * <p>Business rules (a violating event is rejected; state, data and version stay unchanged):
 * <ul>
 *   <li>{@code submit}: positive quantity; a {@code LIMIT} order needs a limit price.
 *   <li>{@code route}: routed quantity never exceeds the order quantity.
 *   <li>{@code fill} / {@code complete_fill}: no overfill, no fill beyond the routed quantity,
 *       {@code complete_fill} exactly fills the order and {@code fill} does not; for {@code LIMIT}
 *       orders a buy fill is at or below the limit, a sell at or above it.
 *   <li>{@code allocate}: allocated quantity never exceeds the filled quantity.
 *   <li>{@code close}: only when allocated quantity equals filled quantity (and matches the command).
 *   <li>every event: {@code accountId}, and symbol/side where present, match the order.
 * </ul>
 */
@LegendModel(files = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"},
        root = "trading::order::Order", javaPackage = "io.concert")
public class TradingOrderMachine extends TradingStateMachine<Order> {

    public static final String TYPE = TradingFlows.ORDER;

    public static final StateMachineSpec SPEC = TradingSpecs.spec(TradingFlows.ORDER_FLOW);

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Order> dataType() {
        return Order.class;
    }

    @Override
    protected Order initialData(String instanceKey) {
        return new Order().setOrderId(instanceKey);
    }

    @Override
    protected void onTransition(String from, String to, Order order, Object payload, EventEnvelope event) {
        if (!(payload instanceof OrderEvent cmd)) {
            reject(event.eventType() + " needs a payload");
            return;
        }
        requireSame("orderId", order.getOrderId(), cmd.getOrderId());
        if (!(cmd instanceof NewOrderCommand)) {
            requireSame("accountId", order.getAccountId(), cmd.getAccountId());
        }
        Instant at = eventTime(cmd);
        order.setStatus(OrderStatus.valueOf(to));
        switch (cmd) {
            case NewOrderCommand c -> {
                requirePositive("quantity", c.getQuantity());
                if (c.getOrderType() == OrderType.LIMIT && c.getLimitPrice() == null) {
                    reject("LIMIT order needs a limitPrice");
                }
                order.setClientId(c.getClientId()).setAccountId(c.getAccountId()).setSymbol(c.getSymbol())
                        .setSide(c.getSide()).setOrderType(c.getOrderType()).setLimitPrice(c.getLimitPrice())
                        .setTimeInForce(c.getTimeInForce()).setCurrency(c.getCurrency()).setQuantity(c.getQuantity())
                        .setLeavesQty(c.getQuantity()).setArrivalPx(c.getArrivalPx()).setCreatedAt(at);
            }
            case AckCommand _ -> order.setAckedAt(at);
            case RejectCommand c -> order.setRejectReason(c.getReason()).setLeavesQty(0L).setCompletedAt(at);
            case RouteCommand c -> {
                requireSame("symbol", order.getSymbol(), c.getSymbol());
                requireSame("side", order.getSide(), c.getSide());
                requirePositive("route quantity", c.getQuantity());
                long routed = order.getRoutedQty() + c.getQuantity();
                if (routed > order.getQuantity()) {
                    reject("over-route: routed " + order.getRoutedQty() + " + " + c.getQuantity() + " > quantity "
                            + order.getQuantity());
                }
                order.setRoutedQty(routed);
            }
            case FillCommand c -> applyFill(order, c, event.eventType().equals("complete_fill"), at);
            case CancelCommand c -> {
                order.setCancelReason(c.getReason()).setLeavesQty(0L);
                if (to.equals(OrderStatus.CANCELLED.name())) {
                    order.setCompletedAt(at);
                }
            }
            case AllocateCommand c -> {
                requireSame("symbol", order.getSymbol(), c.getSymbol());
                requireSame("side", order.getSide(), c.getSide());
                requirePositive("allocation quantity", c.getQuantity());
                long allocated = order.getAllocatedQty() + c.getQuantity();
                if (allocated > order.getFilledQty()) {
                    reject("over-allocation: allocated " + order.getAllocatedQty() + " + " + c.getQuantity()
                            + " > filled " + order.getFilledQty());
                }
                order.setAllocatedQty(allocated);
            }
            case CloseOrderCommand c -> {
                if (!order.getAllocatedQty().equals(order.getFilledQty())) {
                    reject("cannot close: allocated " + order.getAllocatedQty() + " of filled " + order.getFilledQty());
                }
                if (c.getAllocatedQty() != null && !c.getAllocatedQty().equals(order.getAllocatedQty())) {
                    reject("close reports allocated " + c.getAllocatedQty() + ", order has " + order.getAllocatedQty());
                }
                order.setCompletedAt(at);
            }
            default -> reject("unexpected payload " + cmd.getClass().getSimpleName() + " for " + event.eventType());
        }
    }

    /** Quantities, quantity-weighted average price (notional / filled), fees and fill timestamps. */
    private static void applyFill(Order order, FillCommand fill, boolean complete, Instant at) {
        requireSame("symbol", order.getSymbol(), fill.getSymbol());
        requireSame("side", order.getSide(), fill.getSide());
        long filled = checkFill("order " + order.getOrderId(), order.getFilledQty(), order.getQuantity(), fill, complete);
        if (filled > order.getRoutedQty()) {
            reject("fill beyond routed quantity: filled " + filled + " > routed " + order.getRoutedQty());
        }
        checkLimit(order.getOrderType(), order.getSide(), order.getLimitPrice(), fill);
        BigDecimal notional = order.getNotional().add(fill.getPrice().multiply(BigDecimal.valueOf(fill.getQuantity())));
        order.setFilledQty(filled).setLeavesQty(order.getQuantity() - filled).setNotional(notional)
                .setAvgPx(average(notional, filled))
                .setTotalFees(order.getTotalFees().add(fill.getFee() != null ? fill.getFee() : BigDecimal.ZERO))
                .setLastFillAt(at);
        if (order.getFirstFillAt() == null) {
            order.setFirstFillAt(at);
        }
    }
}
