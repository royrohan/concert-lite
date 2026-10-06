// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as OrderAcceptedHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.inventory.ReserveInventoryEvent;
import io.concert.eco.order_events.model.orders.Order;
import io.concert.eco.order_events.model.orders.OrderAcceptedEvent;
import io.concert.eco.order_events.model.orders.OrderExpiryCheckEvent;
import io.concert.eco.order_events.model.orders.OrderLine;
import io.concert.eco.order_events.model.orders.OrderStatus;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import io.concert.sdk.events.NonBlockingError;
import java.time.Instant;

/**
 * Handles {@code OrderAcceptedEvent} (domain {@code orders}, locks {@code order:{orderId}}): marks the order ACCEPTED,
 * emits one {@code ReserveInventoryEvent} per line (domain {@code inventory}, locked on {@code sku:{sku}}) and schedules
 * an {@code OrderExpiryCheckEvent} {@code expirySeconds} from now ({@code ctx.emitAt}: SCHEDULED until then, holding no
 * locks).
 */
@Handles(OrderAcceptedEvent.class)
public class OrderAcceptedHandler implements EventHandler<OrderAcceptedEvent> {

    @Override
    public void apply(OrderAcceptedEvent event, EventContext ctx) {
        Order order = ctx.state(Order.class, "order:" + event.getOrderId())
                .orElseThrow(() -> new NonBlockingError("no order " + event.getOrderId()));
        if (order.getStatus() != OrderStatus.CREATED) {
            throw new NonBlockingError("order " + event.getOrderId() + " is " + order.getStatus() + ", not CREATED");
        }
        Instant now = Instant.now();
        Instant expiresAt = now.plusSeconds(Math.max(1, event.getExpirySeconds()));
        ctx.save(order.setStatus(OrderStatus.ACCEPTED).setUpdatedAt(now).setExpiresAt(expiresAt));
        for (OrderLine line : event.getLines()) {
            ctx.emit(new ReserveInventoryEvent()
                    .setOrderId(event.getOrderId())
                    .setSku(line.getSku())
                    .setQuantity(line.getQuantity()));
        }
        ctx.emitAt(expiresAt, new OrderExpiryCheckEvent().setOrderId(event.getOrderId()));
    }
}
