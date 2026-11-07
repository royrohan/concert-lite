// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as RsTradingExecutionMachine.java.bak-<timestamp>).
package io.concert.eco.trading_gen_rs;

import io.concert.common.EventEnvelope;
import io.concert.eco.trading_gen_rs.model.trading.command.CancelCommand;
import io.concert.eco.trading_gen_rs.model.trading.command.FillCommand;
import io.concert.eco.trading_gen_rs.model.trading.command.RejectCommand;
import io.concert.eco.trading_gen_rs.model.trading.command.RouteCommand;
import io.concert.eco.trading_gen_rs.model.trading.order.Execution;
import io.concert.eco.trading_gen_rs.model.trading.order.ExecutionStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;

/**
 * The {@code rs_trading_execution} state machine: entity data {@code trading::order::Execution}, transitions
 * from {@link RsTradingExecutionSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING_NEW -route-&gt; ROUTED : trading::command::RouteCommand
 *   ROUTED -fill-&gt; PARTIALLY_FILLED : trading::command::FillCommand
 *   ROUTED -complete_fill-&gt; FILLED : trading::command::FillCommand
 *   ROUTED -cancel-&gt; CANCELLED : trading::command::CancelCommand
 *   ROUTED -reject-&gt; REJECTED : trading::command::RejectCommand
 *   PARTIALLY_FILLED -fill-&gt; PARTIALLY_FILLED : trading::command::FillCommand
 *   PARTIALLY_FILLED -complete_fill-&gt; FILLED : trading::command::FillCommand
 *   PARTIALLY_FILLED -cancel-&gt; CANCELLED : trading::command::CancelCommand
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class RsTradingExecutionMachine extends ModelStateMachine<Execution> {

    @Override
    protected StateMachineSpec spec() {
        return RsTradingExecutionSpec.SPEC;
    }

    @Override
    protected Class<Execution> dataType() {
        return Execution.class;
    }

    @Override
    protected Execution initialData(String instanceKey) {
        return new Execution().setExecId(instanceKey).setStatus(ExecutionStatus.PENDING_NEW);
    }

    @Override
    protected void onTransition(String from, String to, Execution execution, Object payload, EventEnvelope event) {
        execution.setStatus(ExecutionStatus.valueOf(to));
        switch (payload) {
            case RouteCommand cmd -> {
                // TODO PENDING_NEW -route-> ROUTED: apply the command to the execution, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the execution
                if (cmd.getOrderId() != null) {
                    execution.setOrderId(cmd.getOrderId());
                }
                if (cmd.getVenue() != null) {
                    execution.setVenue(cmd.getVenue());
                }
                if (cmd.getSymbol() != null) {
                    execution.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    execution.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    execution.setQuantity(cmd.getQuantity());
                }
                if (cmd.getLimitPrice() != null) {
                    execution.setLimitPrice(cmd.getLimitPrice());
                }
            }
            case FillCommand cmd -> {
                // TODO ROUTED -fill-> PARTIALLY_FILLED: apply the command to the execution, call activities, or reject("...")
                // TODO ROUTED -complete_fill-> FILLED: apply the command to the execution, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -fill-> PARTIALLY_FILLED: apply the command to the execution, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -complete_fill-> FILLED: apply the command to the execution, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the execution
                if (cmd.getOrderId() != null) {
                    execution.setOrderId(cmd.getOrderId());
                }
                if (cmd.getSymbol() != null) {
                    execution.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    execution.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    execution.setQuantity(cmd.getQuantity());
                }
                if (cmd.getVenue() != null) {
                    execution.setVenue(cmd.getVenue());
                }
            }
            case CancelCommand cmd -> {
                // TODO ROUTED -cancel-> CANCELLED: apply the command to the execution, call activities, or reject("...")
                // TODO PARTIALLY_FILLED -cancel-> CANCELLED: apply the command to the execution, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the execution
                if (cmd.getOrderId() != null) {
                    execution.setOrderId(cmd.getOrderId());
                }
            }
            case RejectCommand cmd -> {
                // TODO ROUTED -reject-> REJECTED: apply the command to the execution, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the execution
                if (cmd.getOrderId() != null) {
                    execution.setOrderId(cmd.getOrderId());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
