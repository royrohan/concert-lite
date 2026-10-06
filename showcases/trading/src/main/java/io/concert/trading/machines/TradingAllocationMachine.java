package io.concert.trading.machines;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.ConfirmAllocationCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.order.Allocation;
import io.concert.trading.order.AllocationStatus;

/**
 * {@code trading_allocation}: part of a filled order booked to an account; {@code CONFIRMED} is
 * terminal. Rules: positive quantity and average price; {@code confirm} names the same order and
 * receiving account. That the allocations of an order sum to at most its filled quantity is enforced
 * by the order machine (its {@code allocate} event).
 */
@LegendModel(files = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"},
        root = "trading::order::Allocation", javaPackage = "io.concert")
public class TradingAllocationMachine extends TradingStateMachine<Allocation> {

    public static final String TYPE = TradingFlows.ALLOCATION;

    public static final StateMachineSpec SPEC = TradingSpecs.spec(TradingFlows.ALLOCATION_FLOW);

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Allocation> dataType() {
        return Allocation.class;
    }

    @Override
    protected Allocation initialData(String instanceKey) {
        return new Allocation().setAllocId(instanceKey);
    }

    @Override
    protected void onTransition(String from, String to, Allocation alloc, Object payload, EventEnvelope event) {
        if (!(payload instanceof OrderEvent cmd)) {
            reject(event.eventType() + " needs a payload");
            return;
        }
        requireSame("orderId", alloc.getOrderId(), cmd.getOrderId());
        alloc.setStatus(AllocationStatus.valueOf(to));
        switch (cmd) {
            case AllocateCommand c -> {
                requireSame("allocId", alloc.getAllocId(), c.getAllocId());
                requirePositive("allocation quantity", c.getQuantity());
                if (c.getAvgPx() == null || c.getAvgPx().signum() <= 0) {
                    reject("allocation avgPx must be positive, got " + c.getAvgPx());
                }
                alloc.setOrderId(c.getOrderId()).setAccountId(c.getAllocAccountId()).setSymbol(c.getSymbol())
                        .setSide(c.getSide()).setQuantity(c.getQuantity()).setAvgPx(c.getAvgPx())
                        .setAllocatedAt(eventTime(c));
            }
            case ConfirmAllocationCommand c -> {
                requireSame("allocId", alloc.getAllocId(), c.getAllocId());
                requireSame("allocAccountId", alloc.getAccountId(), c.getAllocAccountId());
                alloc.setConfirmedAt(eventTime(c));
            }
            default -> reject("unexpected payload " + cmd.getClass().getSimpleName() + " for " + event.eventType());
        }
    }
}
