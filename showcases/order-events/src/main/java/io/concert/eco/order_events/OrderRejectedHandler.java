// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as OrderRejectedHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.orders.Order;
import io.concert.eco.order_events.model.orders.OrderRejectedEvent;
import io.concert.eco.order_events.model.orders.OrderStatus;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import java.time.Instant;

/** Handles {@code OrderRejectedEvent} (domain {@code orders}, locks {@code order:{orderId}}): marks the order REJECTED. */
@Handles(OrderRejectedEvent.class)
public class OrderRejectedHandler implements EventHandler<OrderRejectedEvent> {

    @Override
    public void apply(OrderRejectedEvent event, EventContext ctx) {
        Order order = ctx.state(Order.class, "order:" + event.getOrderId())
                .orElseGet(() -> new Order().setOrderId(event.getOrderId()).setCustomerId(event.getCustomerId()).setCurrency("USD"));
        ctx.save(order.setStatus(OrderStatus.REJECTED).setRejectReason(event.getReason()).setUpdatedAt(Instant.now()));
    }
}
