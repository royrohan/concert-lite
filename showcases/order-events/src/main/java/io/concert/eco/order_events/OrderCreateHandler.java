// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as OrderCreateHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.orders.Order;
import io.concert.eco.order_events.model.orders.OrderAcceptedEvent;
import io.concert.eco.order_events.model.orders.OrderCreateEvent;
import io.concert.eco.order_events.model.orders.OrderLine;
import io.concert.eco.order_events.model.orders.OrderRejectedEvent;
import io.concert.eco.order_events.model.orders.OrderStatus;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import io.concert.sdk.events.NonBlockingError;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Handles {@code OrderCreateEvent} (domain {@code orders}, locks {@code order:{orderId}, customer:{customerId}}):
 * validates the order, saves the {@code Order} document ({@code state:order:<orderId>}, status CREATED) and emits
 * {@code OrderAcceptedEvent} or {@code OrderRejectedEvent}.
 *
 * <p>Rejected: no lines, a line with quantity &lt;= 0 or a negative price, a customer id starting with
 * {@code BLOCKED}, or a total over {@value #CREDIT_LIMIT}. An order id that already exists parks the event
 * (NonBlockingError).
 */
@Handles(OrderCreateEvent.class)
public class OrderCreateHandler implements EventHandler<OrderCreateEvent> {

    static final long CREDIT_LIMIT = 100_000;

    @Override
    public void apply(OrderCreateEvent event, EventContext ctx) {
        String key = "order:" + event.getOrderId();
        ctx.state(Order.class, key).ifPresent(existing -> {
            throw new NonBlockingError("order " + event.getOrderId() + " already exists (" + existing.getStatus() + ")");
        });
        BigDecimal total = BigDecimal.ZERO;
        String reason = null;
        if (event.getLines().isEmpty()) {
            reason = "no order lines";
        }
        for (OrderLine line : event.getLines()) {
            if (line.getQuantity() <= 0) {
                reason = "quantity of " + line.getSku() + " must be positive";
            } else if (line.getUnitPrice().signum() < 0) {
                reason = "price of " + line.getSku() + " must not be negative";
            } else {
                total = total.add(line.getUnitPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
            }
        }
        if (reason == null && event.getCustomerId().startsWith("BLOCKED")) {
            reason = "customer " + event.getCustomerId() + " is blocked";
        }
        if (reason == null && total.compareTo(BigDecimal.valueOf(CREDIT_LIMIT)) > 0) {
            reason = "total " + total + " " + event.getCurrency() + " is over the credit limit";
        }
        Instant now = Instant.now();
        ctx.save(new Order()
                .setOrderId(event.getOrderId())
                .setCustomerId(event.getCustomerId())
                .setStatus(OrderStatus.CREATED)
                .setCurrency(event.getCurrency())
                .setLines(event.getLines())
                .setTotal(total)
                .setCreatedAt(now)
                .setUpdatedAt(now));
        if (reason != null) {
            ctx.emit(new OrderRejectedEvent()
                    .setOrderId(event.getOrderId())
                    .setCustomerId(event.getCustomerId())
                    .setReason(reason));
        } else {
            ctx.emit(new OrderAcceptedEvent()
                    .setOrderId(event.getOrderId())
                    .setCustomerId(event.getCustomerId())
                    .setLines(event.getLines())
                    .setExpirySeconds(event.getExpirySeconds()));
        }
    }
}
