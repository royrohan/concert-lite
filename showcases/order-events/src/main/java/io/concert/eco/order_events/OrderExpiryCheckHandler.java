// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as OrderExpiryCheckHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.orders.Order;
import io.concert.eco.order_events.model.orders.OrderExpiryCheckEvent;
import io.concert.eco.order_events.model.orders.OrderStatus;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import java.time.Instant;

/**
 * Handles {@code OrderExpiryCheckEvent} (domain {@code orders}, locks {@code order:{orderId}}), scheduled by
 * {@link OrderAcceptedHandler}: an order still ACCEPTED (not paid) when it fires becomes EXPIRED; otherwise nothing
 * changes.
 */
@Handles(OrderExpiryCheckEvent.class)
public class OrderExpiryCheckHandler implements EventHandler<OrderExpiryCheckEvent> {

    @Override
    public void apply(OrderExpiryCheckEvent event, EventContext ctx) {
        ctx.state(Order.class, "order:" + event.getOrderId())
                .filter(order -> order.getStatus() == OrderStatus.ACCEPTED)
                .ifPresent(order -> ctx.save(order.setStatus(OrderStatus.EXPIRED).setUpdatedAt(Instant.now())));
    }
}
