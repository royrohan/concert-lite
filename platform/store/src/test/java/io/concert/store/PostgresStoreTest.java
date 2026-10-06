package io.concert.store;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** STORE_KIND=postgres against a real Postgres 16 (the DSQL stand-in). Needs Docker. */
class PostgresStoreTest extends StateStoreContract {

    static GenericContainer<?> pg;
    static StateStore store;

    @BeforeAll
    static void start() {
        pg = new GenericContainer<>("postgres:16")
                .withEnv("POSTGRES_USER", "concert").withEnv("POSTGRES_PASSWORD", "concert").withEnv("POSTGRES_DB", "concert")
                .withExposedPorts(5432)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2));
        pg.start();
        System.setProperty("STORE_JDBC_URL", "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/concert");
        store = Stores.create("postgres");
    }

    @AfterAll
    static void stop() {
        store.close();
        pg.stop();
    }

    @Override
    protected StateStore store() {
        return store;
    }
}
