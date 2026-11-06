package io.concert.marketdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The control endpoint's semantics: paused emits nothing, start emits at the rate, stop halts without a burst on resume. */
class MarketDataControlTest {

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return cond.getAsBoolean();
    }

    @Test
    void pausedEmitsNothingAndStartStopToggleTicks() throws Exception {
        AtomicLong ticks = new AtomicLong();
        RecordSink sink = (topic, key, value) -> {
            if (topic.equals(MdTopics.TICKS)) {
                ticks.incrementAndGet();
            }
        };
        MarketDataSim sim = new MarketDataSim(MarketDataSim.Config.defaults(), sink);
        MarketDataSim.Control control = new MarketDataSim.Control(false, 1000);
        AtomicBoolean keep = new AtomicBoolean(true);
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                sim.runRealtime(System::currentTimeMillis, keep::get, control);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread.sleep(150);
        assertEquals(0, ticks.get(), "starts paused");
        control.start(2000.0);
        assertTrue(waitFor(() -> ticks.get() > 100, 10_000), "ticks while running: " + ticks.get());
        control.stop();
        Thread.sleep(100); // let the loop see the stop
        long stopped = ticks.get();
        Thread.sleep(300);
        assertEquals(stopped, ticks.get(), "no ticks after stop");
        long resumedAt = System.currentTimeMillis();
        control.start(null);
        assertTrue(waitFor(() -> ticks.get() > stopped, 10_000), "ticks again after start");
        long elapsed = System.currentTimeMillis() - resumedAt;
        // Without re-anchoring, the 400+ ms paused would be made up at once (800+ ticks at 2000/s).
        assertTrue(ticks.get() - stopped <= 2 * elapsed + 50, "no burst on resume: " + (ticks.get() - stopped) + " in " + elapsed + " ms");
        keep.set(false);
        t.join(2000);
    }
}
