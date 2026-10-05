package io.concert.samples;

import io.temporal.activity.ActivityInterface;

/** Stand-in for real side effects (calling a payment API, ...), used by the scaling benchmark. */
@ActivityInterface
public interface SimulatedWork {

    void work(long millis);

    final class Impl implements SimulatedWork {
        @Override
        public void work(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
