// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as RsTradingAllocationMachine.java.bak-<timestamp>).
package io.concert.eco.trading_gen_rs;

import io.concert.common.EventEnvelope;
import io.concert.eco.trading_gen_rs.model.trading.command.AllocateCommand;
import io.concert.eco.trading_gen_rs.model.trading.command.ConfirmAllocationCommand;
import io.concert.eco.trading_gen_rs.model.trading.order.Allocation;
import io.concert.eco.trading_gen_rs.model.trading.order.AllocationStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;

/**
 * The {@code rs_trading_allocation} state machine: entity data {@code trading::order::Allocation}, transitions
 * from {@link RsTradingAllocationSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING -allocate-&gt; ALLOCATED : trading::command::AllocateCommand
 *   ALLOCATED -confirm-&gt; CONFIRMED : trading::command::ConfirmAllocationCommand
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code now()}, the transition's timestamp (decisions may run more than once: no side effects).
 */
public class RsTradingAllocationMachine extends ModelStateMachine<Allocation> {

    @Override
    protected StateMachineSpec spec() {
        return RsTradingAllocationSpec.SPEC;
    }

    @Override
    protected Class<Allocation> dataType() {
        return Allocation.class;
    }

    @Override
    protected Allocation initialData(String instanceKey) {
        return new Allocation().setAllocId(instanceKey).setStatus(AllocationStatus.PENDING);
    }

    @Override
    protected void onTransition(String from, String to, Allocation allocation, Object payload, EventEnvelope event) {
        allocation.setStatus(AllocationStatus.valueOf(to));
        switch (payload) {
            case AllocateCommand cmd -> {
                // TODO PENDING -allocate-> ALLOCATED: apply the command to the allocation, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the allocation
                if (cmd.getOrderId() != null) {
                    allocation.setOrderId(cmd.getOrderId());
                }
                if (cmd.getAccountId() != null) {
                    allocation.setAccountId(cmd.getAccountId());
                }
                if (cmd.getSymbol() != null) {
                    allocation.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    allocation.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    allocation.setQuantity(cmd.getQuantity());
                }
                if (cmd.getAvgPx() != null) {
                    allocation.setAvgPx(cmd.getAvgPx());
                }
            }
            case ConfirmAllocationCommand cmd -> {
                // TODO ALLOCATED -confirm-> CONFIRMED: apply the command to the allocation, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the allocation
                if (cmd.getOrderId() != null) {
                    allocation.setOrderId(cmd.getOrderId());
                }
                if (cmd.getAccountId() != null) {
                    allocation.setAccountId(cmd.getAccountId());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
