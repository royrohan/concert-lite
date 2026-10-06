// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as ReserveInventoryHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.inventory.ReserveInventoryEvent;
import io.concert.eco.order_events.model.inventory.Stock;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import io.concert.sdk.events.NonBlockingError;
import java.time.Instant;

/**
 * Handles {@code ReserveInventoryEvent} (domain {@code inventory}, locks {@code sku:{sku}}): takes the quantity from
 * the SKU's {@code Stock} document. Out of stock is a {@link NonBlockingError}: the event is parked
 * (ERROR_NON_BLOCKING) and {@code sku:<sku>} released; retry it after a {@code RestockEvent}.
 */
@Handles(ReserveInventoryEvent.class)
public class ReserveInventoryHandler implements EventHandler<ReserveInventoryEvent> {

    @Override
    public void apply(ReserveInventoryEvent event, EventContext ctx) {
        String key = "sku:" + event.getSku();
        Stock stock = ctx.state(Stock.class, key).orElse(null);
        long available = stock == null ? 0 : stock.getAvailable();
        if (available < event.getQuantity()) {
            throw new NonBlockingError("out of stock: " + event.getSku() + " needs " + event.getQuantity() + ", has " + available
                    + " (order " + event.getOrderId() + ")");
        }
        ctx.save(stock.setAvailable(available - event.getQuantity())
                .setReserved(stock.getReserved() + event.getQuantity())
                .setUpdatedAt(Instant.now()));
    }
}
