// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as GenTradingSystemMachine.java.bak-<timestamp>).
package io.concert.eco.trading_gen;

import io.concert.common.EventEnvelope;
import io.concert.eco.trading_gen.model.trading.command.ChainStepCommand;
import io.concert.eco.trading_gen.model.trading.command.SubmitTradeCommand;
import io.concert.eco.trading_gen.model.trading.order.Trade;
import io.concert.eco.trading_gen.model.trading.order.TradeStatus;
import io.concert.sdk.ModelStateMachine;
import io.concert.sdk.StateMachineSpec;

/**
 * The {@code gen_trading_system} state machine: entity data {@code trading::order::Trade}, transitions
 * from {@link GenTradingSystemSpec} (regenerated from the Pure annotations):
 *
 * <pre>
 *   PENDING -submit-&gt; SUBMITTED : trading::command::SubmitTradeCommand
 *   SUBMITTED -order_done-&gt; ORDER_DONE : trading::command::ChainStepCommand
 *   SUBMITTED -order_failed-&gt; FAILED : trading::command::ChainStepCommand
 *   ORDER_DONE -execution_done-&gt; EXECUTED : trading::command::ChainStepCommand
 *   EXECUTED -fill_done-&gt; FILLED : trading::command::ChainStepCommand
 *   FILLED -allocation_done-&gt; COMPLETE : trading::command::ChainStepCommand
 * </pre>
 *
 * <p>{@code onTransition} mutates the data in place; reject an event with {@code reject("reason")}. Take time
 * from {@code Workflow.currentTimeMillis()} (workflow code must be deterministic).
 */
public class GenTradingSystemMachine extends ModelStateMachine<Trade> {

    @Override
    protected StateMachineSpec spec() {
        return GenTradingSystemSpec.SPEC;
    }

    @Override
    protected Class<Trade> dataType() {
        return Trade.class;
    }

    @Override
    protected Trade initialData(String instanceKey) {
        return new Trade().setOrderId(instanceKey).setStatus(TradeStatus.PENDING);
    }

    @Override
    protected void onTransition(String from, String to, Trade trade, Object payload, EventEnvelope event) {
        trade.setStatus(TradeStatus.valueOf(to));
        switch (payload) {
            case SubmitTradeCommand cmd -> {
                // TODO PENDING -submit-> SUBMITTED: apply the command to the trade, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the trade
                if (cmd.getAccountId() != null) {
                    trade.setAccountId(cmd.getAccountId());
                }
                if (cmd.getClientId() != null) {
                    trade.setClientId(cmd.getClientId());
                }
                if (cmd.getSymbol() != null) {
                    trade.setSymbol(cmd.getSymbol());
                }
                if (cmd.getSide() != null) {
                    trade.setSide(cmd.getSide());
                }
                if (cmd.getOrderType() != null) {
                    trade.setOrderType(cmd.getOrderType());
                }
                if (cmd.getQuantity() != null) {
                    trade.setQuantity(cmd.getQuantity());
                }
                if (cmd.getLimitPrice() != null) {
                    trade.setLimitPrice(cmd.getLimitPrice());
                }
                if (cmd.getVenue() != null) {
                    trade.setVenue(cmd.getVenue());
                }
                if (cmd.getPrice() != null) {
                    trade.setPrice(cmd.getPrice());
                }
                if (cmd.getFee() != null) {
                    trade.setFee(cmd.getFee());
                }
                if (cmd.getLiquidity() != null) {
                    trade.setLiquidity(cmd.getLiquidity());
                }
                if (cmd.getAllocAccountId() != null) {
                    trade.setAllocAccountId(cmd.getAllocAccountId());
                }
            }
            case ChainStepCommand cmd -> {
                // TODO SUBMITTED -order_done-> ORDER_DONE: apply the command to the trade, call activities, or reject("...")
                // TODO SUBMITTED -order_failed-> FAILED: apply the command to the trade, call activities, or reject("...")
                // TODO ORDER_DONE -execution_done-> EXECUTED: apply the command to the trade, call activities, or reject("...")
                // TODO EXECUTED -fill_done-> FILLED: apply the command to the trade, call activities, or reject("...")
                // TODO FILLED -allocation_done-> COMPLETE: apply the command to the trade, call activities, or reject("...")
                // generated starting point: copy the fields the command shares with the trade
                if (cmd.getAccountId() != null) {
                    trade.setAccountId(cmd.getAccountId());
                }
                if (cmd.getStep() != null) {
                    trade.setStep(cmd.getStep());
                }
                if (cmd.getRefId() != null) {
                    trade.setRefId(cmd.getRefId());
                }
                if (cmd.getState() != null) {
                    trade.setState(cmd.getState());
                }
                if (cmd.getQuantity() != null) {
                    trade.setQuantity(cmd.getQuantity());
                }
                if (cmd.getPrice() != null) {
                    trade.setPrice(cmd.getPrice());
                }
            }
            case null, default -> reject("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }
}
