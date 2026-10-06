// Yours to edit: generated once by ./generate-concert-ecosystem and never overwritten (--force
// regenerates it after backing it up as PaymentCaptureHandler.java.bak-<timestamp>).
package io.concert.eco.order_events;

import io.concert.eco.order_events.model.orders.Order;
import io.concert.eco.order_events.model.orders.OrderStatus;
import io.concert.eco.order_events.model.orders.PaymentCaptureEvent;
import io.concert.sdk.events.BlockingError;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.Handles;
import io.concert.sdk.events.NonBlockingError;
import java.time.Instant;

/**
 * Handles {@code PaymentCaptureEvent} (domain {@code orders}, locks {@code order:{orderId}}): marks an ACCEPTED order
 * PAID.
 *
 * <p>The BLOCKING demo: {@code poison = true} throws a {@link BlockingError}. The event goes ERROR_BLOCKING and keeps
 * {@code order:<orderId>} held, so every later event of that order (another payment, the expiry check) waits until an
 * operator skips it (or retries it, which fails again while the payload is poisoned).
 */
@Handles(PaymentCaptureEvent.class)
public class PaymentCaptureHandler implements EventHandler<PaymentCaptureEvent> {

    @Override
    public void apply(PaymentCaptureEvent event, EventContext ctx) {
        if (Boolean.TRUE.equals(event.getPoison())) {
            throw new BlockingError("payment provider rejected the capture for order " + event.getOrderId() + " (poison payload)");
        }
        Order order = ctx.state(Order.class, "order:" + event.getOrderId())
                .orElseThrow(() -> new NonBlockingError("no order " + event.getOrderId()));
        switch (order.getStatus()) {
            case ACCEPTED -> ctx.save(order.setStatus(OrderStatus.PAID).setPaidAmount(event.getAmount()).setUpdatedAt(Instant.now()));
            case PAID -> { } // already paid: nothing to do
            default -> throw new NonBlockingError("order " + event.getOrderId() + " is " + order.getStatus() + ": cannot capture a payment");
        }
    }
}
