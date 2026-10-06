// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as GenTradingOrderMachine.java.bak-<timestamp>).
package io.concert.eco.trading_gen;

import io.concert.common.EventEnvelope;
import io.concert.eco.trading_gen.model.trading.command.AckCommand;
import io.concert.eco.trading_gen.model.trading.command.AllocateCommand;
import io.concert.eco.trading_gen.model.trading.command.CancelCommand;
import io.concert.eco.trading_gen.model.trading.command.CloseOrderCommand;
import io.concert.eco.trading_gen.model.trading.command.FillCommand;
import io.concert.eco.trading_gen.model.trading.command.NewOrderCommand;
import io.concert.eco.trading_gen.model.trading.command.RejectCommand;
import io.concert.eco.trading_gen.model.trading.command.RouteCommand;
import io.concert.eco.trading_gen.model.trading.order.Order;
import io.concert.eco.trading_gen.model.trading.order.OrderStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;

/**
 * The {@code gen_trading_order} state machine: entity data {@code trading::order::Order}, transitions
 * from {@link GenTradingOrderSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING_NEW -submit-&gt; NEW : trading::command::NewOrderCommand
 *   NEW -ack-&gt; ACKED : trading::command::AckCommand
 *   NEW -reject-&gt; REJECTED : trading::command::RejectCommand
 *   ACKED -route-&gt; WORKING : trading::command::RouteCommand
 *   ACKED -cancel-&gt; CANCELLED : trading::command::CancelCommand
 *   WORKING -route-&gt; WORKING : trading::command::RouteCommand
 *   WORKING -fill-&gt; PARTIALLY_FILLED : trading::command::FillCommand
 *   WORKING -complete_fill-&gt; ALLOCATING : trading::command::FillCommand
 *   WORKING -cancel-&gt; CANCELLED : trading::command::CancelCommand
 *   PARTIALLY_FILLED -route-&gt; PARTIALLY_FILLED : trading::command::RouteCommand
 *   PARTIALLY_FILLED -fill-&gt; PARTIALLY_FILLED : trading::command::FillCommand
 *   PARTIALLY_FILLED -complete_fill-&gt; ALLOCATING : trading::command::FillCommand
 *   PARTIALLY_FILLED -cancel-&gt; CANCEL_ALLOCATING : trading::command::CancelCommand
 *   ALLOCATING -allocate-&gt; ALLOCATING : trading::command::AllocateCommand
 *   ALLOCATING -close-&gt; FILLED : trading::command::CloseOrderCommand
 *   CANCEL_ALLOCATING -allocate-&gt; CANCEL_ALLOCATING : trading::command::AllocateCommand
 *   CANCEL_ALLOCATING -close-&gt; CANCELLED : trading::command::CloseOrderCommand
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class GenTradingOrderMachine extends ModelStateMachine<Order> {

    @Override
    protected StateMachineSpec spec() {
        return GenTradingOrderSpec.SPEC;
    }

    @Override
    protected Class<Order> dataType() {
        return Order.class;
    }

    @Override
    protected Order initialData(String instanceKey) {
        return new Order().setOrderId(instanceKey).setStatus(OrderStatus.PENDING_NEW);
    }

    @Override
    protected void onTransition(String from, String to, Order order, Object payload, EventEnvelope event) {
        order.setStatus(OrderStatus.valueOf(to));
        switch (payload) {
            case NewOrderCommand cmd -> {
                // TODO PENDING_NEW -submit-> NEW: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
                if (cmd.getClientId() != null) {
                    order.setClientId(cmd.getClientId());
                }
                if (cmd.getSymbol() != null) {
                    order.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    order.setSide(cmd.getSide());
                }
                if (cmd.getOrderType() != null) {
                    order.setOrderType(cmd.getOrderType());
                }
                if (cmd.getLimitPrice() != null) {
                    order.setLimitPrice(cmd.getLimitPrice());
                }
                if (cmd.getTimeInForce() != null) {
                    order.setTimeInForce(cmd.getTimeInForce());
                }
                if (cmd.getCurrency() != null) {
                    order.setCurrency(cmd.getCurrency());
                }
                if (cmd.getQuantity() != null) {
                    order.setQuantity(cmd.getQuantity());
                }
                if (cmd.getArrivalPx() != null) {
                    order.setArrivalPx(cmd.getArrivalPx());
                }
            }
            case AckCommand cmd -> {
                // TODO NEW -ack-> ACKED: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
            }
            case RejectCommand cmd -> {
                // TODO NEW -reject-> REJECTED: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
            }
            case RouteCommand cmd -> {
                // TODO ACKED -route-> WORKING: apply the command to the order, call activities, or reject("...")
                // TODO WORKING -route-> WORKING: apply the command to the order, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -route-> PARTIALLY_FILLED: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
                if (cmd.getSymbol() != null) {
                    order.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    order.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    order.setQuantity(cmd.getQuantity());
                }
                if (cmd.getLimitPrice() != null) {
                    order.setLimitPrice(cmd.getLimitPrice());
                }
            }
            case CancelCommand cmd -> {
                // TODO ACKED -cancel-> CANCELLED: apply the command to the order, call activities, or reject("...")
                // TODO WORKING -cancel-> CANCELLED: apply the command to the order, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -cancel-> CANCEL_ALLOCATING: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
            }
            case FillCommand cmd -> {
                // TODO WORKING -fill-> PARTIALLY_FILLED: apply the command to the order, call activities, or reject("...")
                // TODO WORKING -complete_fill-> ALLOCATING: apply the command to the order, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -fill-> PARTIALLY_FILLED: apply the command to the order, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -complete_fill-> ALLOCATING: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
                if (cmd.getSymbol() != null) {
                    order.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    order.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    order.setQuantity(cmd.getQuantity());
                }
            }
            case AllocateCommand cmd -> {
                // TODO ALLOCATING -allocate-> ALLOCATING: apply the command to the order, call activities, or reject("...")
                // TODO CANCEL_ALLOCATING -allocate-> CANCEL_ALLOCATING: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
                if (cmd.getSymbol() != null) {
                    order.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    order.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    order.setQuantity(cmd.getQuantity());
                }
                if (cmd.getAvgPx() != null) {
                    order.setAvgPx(cmd.getAvgPx());
                }
            }
            case CloseOrderCommand cmd -> {
                // TODO ALLOCATING -close-> FILLED: apply the command to the order, call activities, or reject("...")
                // TODO CANCEL_ALLOCATING -close-> CANCELLED: apply the command to the order, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the order
                if (cmd.getAccountId() != null) {
                    order.setAccountId(cmd.getAccountId());
                }
                if (cmd.getAllocatedQty() != null) {
                    order.setAllocatedQty(cmd.getAllocatedQty());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
