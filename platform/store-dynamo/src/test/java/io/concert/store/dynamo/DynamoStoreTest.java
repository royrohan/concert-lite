package io.concert.store.dynamo;

import io.concert.store.StateStore;
import io.concert.store.StateStoreContract;
import io.concert.store.Stores;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** STORE_KIND=dynamo against DynamoDB on LocalStack. Needs Docker. */
class DynamoStoreTest extends StateStoreContract {

    static GenericContainer<?> localstack;
    static StateStore store;

    @BeforeAll
    static void start() {
        localstack = new GenericContainer<>("localstack/localstack:4.12")
                .withEnv("SERVICES", "dynamodb")
                .withExposedPorts(4566)
                .waitingFor(Wait.forHttp("/_localstack/health").forPort(4566)
                        .forResponsePredicate(b -> b.contains("\"dynamodb\": \"available\"") || b.contains("\"dynamodb\": \"running\""))
                        .withStartupTimeout(Duration.ofMinutes(3)));
        localstack.start();
        System.setProperty("DYNAMO_ENDPOINT", "http://" + localstack.getHost() + ":" + localstack.getMappedPort(4566));
        store = Stores.create("dynamo");
    }

    @AfterAll
    static void stop() {
        store.close();
        localstack.stop();
    }

    @Override
    protected StateStore store() {
        return store;
    }

    @Override
    protected boolean sweepsSynchronously() {
        return false; // native TTL
    }
}
