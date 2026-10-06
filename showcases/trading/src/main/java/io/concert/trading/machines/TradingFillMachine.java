package io.concert.trading.machines;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.FillCommand;
import io.concert.trading.order.Fill;
import io.concert.trading.order.FillStatus;

/**
 * {@code trading_fill}: one venue fill, {@code BOOKED} (terminal) on its only event {@code book}, so
 * its snapshot reaches analytics within one event of the fill. Rules: positive quantity and price,
 * {@code fillId} matches the entity.
 */
@LegendModel(files = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"},
        root = "trading::order::Fill", javaPackage = "io.concert")
public class TradingFillMachine extends TradingStateMachine<Fill> {

    public static final String TYPE = TradingFlows.FILL;

    public static final StateMachineSpec SPEC = TradingSpecs.spec(TradingFlows.FILL_FLOW);

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Fill> dataType() {
        return Fill.class;
    }

    @Override
    protected Fill initialData(String instanceKey) {
        return new Fill().setFillId(instanceKey);
    }

    @Override
    protected void onTransition(String from, String to, Fill fill, Object payload, EventEnvelope event) {
        if (!(payload instanceof FillCommand c)) {
            reject(event.eventType() + " needs a FillCommand");
            return;
        }
        requireSame("fillId", fill.getFillId(), c.getFillId());
        requirePositive("fill quantity", c.getQuantity());
        if (c.getPrice() == null || c.getPrice().signum() <= 0) {
            reject("fill price must be positive, got " + c.getPrice());
        }
        fill.setExecId(c.getExecId()).setOrderId(c.getOrderId()).setAccountId(c.getAccountId()).setSymbol(c.getSymbol())
                .setSide(c.getSide()).setQuantity(c.getQuantity()).setPrice(c.getPrice()).setVenue(c.getVenue())
                .setLiquidity(c.getLiquidity()).setFee(c.getFee()).setTs(eventTime(c))
                .setStatus(FillStatus.valueOf(to));
    }
}
