package io.concert.trading.machines;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.sdk.StateMachineSpec;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.CancelCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.command.RejectCommand;
import io.concert.trading.command.RouteCommand;
import io.concert.trading.common.OrderType;
import io.concert.trading.order.Execution;
import io.concert.trading.order.ExecutionStatus;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * {@code trading_execution}: a child order routed to one venue. Keeps aggregates only (the generated
 * {@code fills} list stays empty; fills are {@code trading_fill} snapshots joined by {@code execId}).
 * Rules: positive route quantity; no overfill of the execution; {@code complete_fill} fills it exactly
 * and {@code fill} does not; fills respect the routed limit price (if any; an execution with a limit
 * price is treated as a limit child order).
 *
 * <p>The execution has no notional field, so {@code avgPx} is updated incrementally
 * ({@code (avgPx * filled + px * qty) / (filled + qty)} at 6 decimals): within 1e-6 of the exact
 * average. The order's {@code avgPx} is exact (from its notional).
 */
@LegendModel(files = {"common.pure", "refdata.pure", "marketdata.pure", "order.pure", "commands.pure"},
        root = "trading::order::Execution", javaPackage = "io.concert")
public class TradingExecutionMachine extends TradingStateMachine<Execution> {

    public static final String TYPE = TradingFlows.EXECUTION;

    public static final StateMachineSpec SPEC = TradingSpecs.spec(TradingFlows.EXECUTION_FLOW);

    @Override
    protected StateMachineSpec spec() {
        return SPEC;
    }

    @Override
    protected Class<Execution> dataType() {
        return Execution.class;
    }

    @Override
    protected Execution initialData(String instanceKey) {
        return new Execution().setExecId(instanceKey);
    }

    @Override
    protected void onTransition(String from, String to, Execution exec, Object payload, EventEnvelope event) {
        if (!(payload instanceof OrderEvent cmd)) {
            reject(event.eventType() + " needs a payload");
            return;
        }
        requireSame("orderId", exec.getOrderId(), cmd.getOrderId());
        Instant at = eventTime(cmd);
        exec.setStatus(ExecutionStatus.valueOf(to));
        switch (cmd) {
            case RouteCommand c -> {
                requireSame("execId", exec.getExecId(), c.getExecId());
                requirePositive("route quantity", c.getQuantity());
                exec.setOrderId(c.getOrderId()).setSymbol(c.getSymbol()).setSide(c.getSide()).setVenue(c.getVenue())
                        .setQuantity(c.getQuantity()).setLimitPrice(c.getLimitPrice()).setRoutedAt(at);
            }
            case FillCommand c -> {
                requireSame("execId", exec.getExecId(), c.getExecId());
                requireSame("symbol", exec.getSymbol(), c.getSymbol());
                requireSame("side", exec.getSide(), c.getSide());
                boolean complete = event.eventType().equals("complete_fill");
                long filled = checkFill("execution " + exec.getExecId(), exec.getFilledQty(), exec.getQuantity(), c, complete);
                checkLimit(OrderType.LIMIT, exec.getSide(), exec.getLimitPrice(), c);
                BigDecimal prior = exec.getAvgPx() == null ? BigDecimal.ZERO
                        : exec.getAvgPx().multiply(BigDecimal.valueOf(exec.getFilledQty()));
                exec.setAvgPx(average(prior.add(c.getPrice().multiply(BigDecimal.valueOf(c.getQuantity()))), filled))
                        .setFilledQty(filled)
                        .setTotalFees(exec.getTotalFees().add(c.getFee() != null ? c.getFee() : BigDecimal.ZERO));
                if (complete) {
                    exec.setCompletedAt(at);
                }
            }
            case CancelCommand c -> {
                requireSame("execId", exec.getExecId(), c.getExecId());
                exec.setCompletedAt(at);
            }
            case RejectCommand c -> {
                requireSame("execId", exec.getExecId(), c.getExecId());
                exec.setCompletedAt(at);
            }
            default -> reject("unexpected payload " + cmd.getClass().getSimpleName() + " for " + event.eventType());
        }
    }
}
