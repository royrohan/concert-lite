package io.concert.store.spanner;

import io.concert.store.StateStore;
import io.concert.store.StateStoreContract;
import io.concert.store.Stores;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** STORE_KIND=spanner against the official Cloud Spanner emulator. Needs Docker. */
class SpannerStoreTest extends StateStoreContract {

    static GenericContainer<?> emulator;
    static StateStore store;

    @BeforeAll
    static void start() {
        emulator = new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator:latest")
                .withExposedPorts(9010, 9020)
                .waitingFor(Wait.forLogMessage(".*gRPC server listening.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        emulator.start();
        System.setProperty("SPANNER_EMULATOR_HOST", emulator.getHost() + ":" + emulator.getMappedPort(9010));
        store = Stores.create("spanner");
    }

    @AfterAll
    static void stop() {
        store.close();
        emulator.stop();
    }

    @Override
    protected StateStore store() {
        return store;
    }
}
