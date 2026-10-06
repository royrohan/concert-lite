package io.concert.samples;

import io.concert.common.EventEnvelope;
import io.concert.model.runtime.LegendModel;
import io.concert.samples.model.demo.common.Money;
import io.concert.samples.model.demo.common.PaymentMethod;
import io.concert.samples.model.demo.order.CancelCommand;
import io.concert.samples.model.demo.order.DeliverCommand;
import io.concert.samples.model.demo.order.Order;
import io.concert.samples.model.demo.order.OrderLine;
import io.concert.samples.model.demo.order.OrderStatus;
import io.concert.samples.model.demo.order.PayCommand;
import io.concert.samples.model.demo.order.RefundCommand;
import io.concert.samples.model.demo.order.ShipCommand;
import io.concert.sdk.StateMachineSpec;
import java.util.List;

/** Typed order machine: entity data is {@link Order} from {@code order.pure}, payloads are its commands. */
@LegendModel(files = {"common.pure", "order.pure"}, root = "demo::order::Order")
public class OrderStateMachine extends SampleModelStateMachine<Order> {

    public static final String TYPE = "order";

    public static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CREATED")
            .on("CREATED", "pay", "PAID", PayCommand.class)
            .on("CREATED", "cancel", "CANCELLED", CancelCommand.class)
            .on("PAID", "ship", "SHIPPED", ShipCommand.class)
            .on("PAID", "cancel", "REFUND_PENDING", CancelCommand.class)
            .on("SHIPPED", "deliver", "DELIVERED", DeliverCommand.class)
            .on("REFUND_PENDING", "refunded", "CANCELLED", RefundCommand.class)
            .build();

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

    /**
     * Defaults for a missing command or missing required fields: {@code pay} takes the order total (else
     * the total of the command's lines, else 0 USD) and {@code CARD}; {@code ship} uses carrier
     * {@code UNASSIGNED} and tracking number {@code TRK-<orderId>}; {@code cancel} the reason
     * {@code unspecified}; {@code refunded} the order total (else 0 USD).
     */
    @Override
    protected Object completePayload(String fromState, String eventType, Object payload, Order order) {
        return switch (eventType) {
            case "pay" -> {
                PayCommand pay = payload != null ? (PayCommand) payload : new PayCommand();
                if (pay.getAmount() == null) {
                    Money lines = lineTotal(pay.getLines());
                    pay.setAmount(Moneys.copy(order.getTotal() != null ? order.getTotal() : lines != null ? lines : Moneys.zero()));
                }
                if (pay.getMethod() == null) {
                    pay.setMethod(PaymentMethod.CARD);
                }
                yield pay;
            }
            case "ship" -> {
                ShipCommand ship = payload != null ? (ShipCommand) payload : new ShipCommand();
                if (ship.getCarrier() == null) {
                    ship.setCarrier("UNASSIGNED");
                }
                if (ship.getTrackingNumber() == null) {
                    ship.setTrackingNumber("TRK-" + order.getOrderId());
                }
                yield ship;
            }
            case "deliver" -> payload != null ? payload : new DeliverCommand();
            case "cancel" -> {
                CancelCommand cancel = payload != null ? (CancelCommand) payload : new CancelCommand();
                yield cancel.getReason() != null ? cancel : cancel.setReason("unspecified");
            }
            case "refunded" -> {
                RefundCommand refund = payload != null ? (RefundCommand) payload : new RefundCommand();
                yield refund.getAmount() != null ? refund
                        : refund.setAmount(Moneys.copy(order.getTotal() != null ? order.getTotal() : Moneys.zero()));
            }
            default -> payload;
        };
    }

    @Override
    protected void onTransition(String from, String to, Order order, Object payload, EventEnvelope event) {
        order.setStatus(OrderStatus.valueOf(to));
        switch (payload) {
            case PayCommand pay -> {
                if (!pay.getLines().isEmpty()) {
                    Money lines = lineTotal(pay.getLines());
                    if (lines == null) {
                        reject("order lines mix currencies");
                    }
                    pay.getLines().forEach(order::addLine);
                    order.setTotal(lines);
                }
                if (order.getTotal() == null) {
                    order.setTotal(Moneys.copy(pay.getAmount()));
                } else if (!Moneys.equal(pay.getAmount(), order.getTotal())) {
                    reject("payment of " + Moneys.format(pay.getAmount()) + " does not match order total "
                            + Moneys.format(order.getTotal()));
                }
                if (pay.getCustomer() != null) {
                    order.setCustomer(pay.getCustomer());
                }
                if (pay.getShippingAddress() != null) {
                    order.setShippingAddress(pay.getShippingAddress());
                }
                order.setPaymentMethod(pay.getMethod()).setPaidAt(now());
                simulateWork(pay.getWorkMs());
            }
            case ShipCommand ship -> {
                order.setCarrier(ship.getCarrier()).setTrackingNumber(ship.getTrackingNumber()).setShippedAt(now());
                simulateWork(ship.getWorkMs());
            }
            case DeliverCommand deliver -> {
                order.setSignedBy(deliver.getSignedBy()).setDeliveredAt(now());
                simulateWork(deliver.getWorkMs());
            }
            case CancelCommand cancel -> {
                order.setCancelReason(cancel.getReason());
                if (to.equals("CANCELLED")) {
                    order.setCancelledAt(now());
                }
                simulateWork(cancel.getWorkMs());
            }
            case RefundCommand refund -> {
                if (order.getTotal() != null && !Moneys.atMost(refund.getAmount(), order.getTotal())) {
                    reject("refund of " + Moneys.format(refund.getAmount()) + " exceeds order total " + Moneys.format(order.getTotal()));
                }
                order.setRefunded(Moneys.copy(refund.getAmount())).setCancelledAt(now());
                simulateWork(refund.getWorkMs());
            }
            case null, default -> throw new IllegalStateException("unexpected payload for " + event.eventType() + ": " + payload);
        }
    }

    /** Sum of quantity x unit price, or {@code null} if there are no lines, a price is missing or currencies differ. */
    private static Money lineTotal(List<OrderLine> lines) {
        if (lines.isEmpty() || lines.stream().anyMatch(l -> l.getUnitPrice() == null || l.getUnitPrice().getAmount() == null
                || l.getUnitPrice().getCurrency() == null || l.getQuantity() == null)) {
            return null;
        }
        return Moneys.sum(lines, l -> Moneys.times(l.getUnitPrice(), l.getQuantity()));
    }
}
