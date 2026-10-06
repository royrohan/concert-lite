// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as RestockHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.inventory.RestockEvent;
import io.concert.eco.order_events.model.inventory.Stock;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import io.concert.sdk.events.NonBlockingError;
import java.time.Instant;

/** Handles {@code RestockEvent} (domain {@code inventory}, locks {@code sku:{sku}}): adds to the SKU's available stock. */
@Handles(RestockEvent.class)
public class RestockHandler implements EventHandler<RestockEvent> {

    @Override
    public void apply(RestockEvent event, EventContext ctx) {
        if (event.getQuantity() <= 0) {
            throw new NonBlockingError("restock quantity must be positive, not " + event.getQuantity());
        }
        String key = "sku:" + event.getSku();
        Stock stock = ctx.state(Stock.class, key).orElseGet(() -> new Stock().setSku(event.getSku()));
        ctx.save(stock.setAvailable(stock.getAvailable() + event.getQuantity())
                .setRestocked(stock.getRestocked() + event.getQuantity())
                .setUpdatedAt(Instant.now()));
    }
}
