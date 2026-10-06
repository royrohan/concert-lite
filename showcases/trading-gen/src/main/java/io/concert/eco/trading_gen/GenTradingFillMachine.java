// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as GenTradingFillMachine.java.bak-<timestamp>).
package io.concert.eco.trading_gen;

import io.concert.common.EventEnvelope;
import io.concert.eco.trading_gen.model.trading.command.FillCommand;
import io.concert.eco.trading_gen.model.trading.order.Fill;
import io.concert.eco.trading_gen.model.trading.order.FillStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;

/**
 * The {@code gen_trading_fill} state machine: entity data {@code trading::order::Fill}, transitions
 * from {@link GenTradingFillSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING -book-&gt; BOOKED : trading::command::FillCommand
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class GenTradingFillMachine extends ModelStateMachine<Fill> {

    @Override
    protected StateMachineSpec spec() {
        return GenTradingFillSpec.SPEC;
    }

    @Override
    protected Class<Fill> dataType() {
        return Fill.class;
    }

    @Override
    protected Fill initialData(String instanceKey) {
        return new Fill().setFillId(instanceKey).setStatus(FillStatus.PENDING);
    }

    @Override
    protected void onTransition(String from, String to, Fill fill, Object payload, EventEnvelope event) {
        fill.setStatus(FillStatus.valueOf(to));
        switch (payload) {
            case FillCommand cmd -> {
                // TODO PENDING -book-> BOOKED: apply the command to the fill, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the fill
                if (cmd.getOrderId() != null) {
                    fill.setOrderId(cmd.getOrderId());
                }
                if (cmd.getAccountId() != null) {
                    fill.setAccountId(cmd.getAccountId());
                }
                if (cmd.getTs() != null) {
                    fill.setTs(cmd.getTs());
                }
                if (cmd.getExecId() != null) {
                    fill.setExecId(cmd.getExecId());
                }
                if (cmd.getSymbol() != null) {
                    fill.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    fill.setSide(cmd.getSide());
                }
                if (cmd.getQuantity() != null) {
                    fill.setQuantity(cmd.getQuantity());
                }
                if (cmd.getPrice() != null) {
                    fill.setPrice(cmd.getPrice());
                }
                if (cmd.getVenue() != null) {
                    fill.setVenue(cmd.getVenue());
                }
                if (cmd.getLiquidity() != null) {
                    fill.setLiquidity(cmd.getLiquidity());
                }
                if (cmd.getFee() != null) {
                    fill.setFee(cmd.getFee());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
