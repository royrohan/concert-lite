package io.concert.it;

import io.concert.common.TemporalClients;
import io.concert.orchestration.KinesisClients;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import io.temporal.client.WorkflowClient;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * Containers shared by all integration tests in the JVM (started once, reaped by Testcontainers):
 * LocalStack for Kinesis, the Temporal dev server with our search attributes, Postgres 16 as the
 * DSQL stand-in.
 */
final class Stack {
    private Stack() {}

    static final GenericContainer<?> POSTGRES = new GenericContainer<>(DockerImageName.parse("postgres:16"))
            .withEnv("POSTGRES_USER", "concert")
            .withEnv("POSTGRES_PASSWORD", "concert")
            .withEnv("POSTGRES_DB", "concert")
            .withCommand("postgres", "-c", "max_connections=300", "-c", "fsync=off")
            .withExposedPorts(5432)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2));

    static final GenericContainer<?> LOCALSTACK = new GenericContainer<>(DockerImageName.parse("localstack/localstack:4.12"))
            .withEnv("SERVICES", "kinesis")
            .withEnv("KINESIS_LATENCY", "0")
            .withExposedPorts(4566)
            .waitingFor(Wait.forHttp("/_localstack/health").forPort(4566)
                    .forResponsePredicate(body -> body.contains("\"kinesis\": \"available\"") || body.contains("\"kinesis\": \"running\""))
                    .withStartupTimeout(Duration.ofMinutes(3)));

    static final GenericContainer<?> TEMPORAL = new GenericContainer<>(DockerImageName.parse("temporalio/temporal:latest"))
            .withCommand("server", "start-dev", "--ip", "0.0.0.0", "--headless",
                    "--dynamic-config-value", "frontend.enableUpdateWorkflowExecution=true",
                    "--dynamic-config-value", "frontend.enableExecuteMultiOperation=true",
                    "--dynamic-config-value", "system.enableActivityEagerExecution=true",
                    "--search-attribute", "EventId=Keyword",
                    "--search-attribute", "SmType=Keyword",
                    "--search-attribute", "InstanceKey=Keyword",
                    "--search-attribute", "LockKeys=KeywordList")
            .withExposedPorts(7233)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

    private static boolean started;

    static synchronized void start() {
        if (started) {
            return;
        }
        POSTGRES.start();
        LOCALSTACK.start();
        TEMPORAL.start();
        System.setProperty("STORE_JDBC_URL",
                "jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(5432) + "/concert");
        System.setProperty("STORE_POOL_SIZE", "48");
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "true");
        started = true;
    }

    static String temporalAddress() {
        return TEMPORAL.getHost() + ":" + TEMPORAL.getMappedPort(7233);
    }

    static WorkflowClient newClient() {
        return TemporalClients.create(temporalAddress(), "default");
    }

    static KinesisClient kinesis() {
        return KinesisClients.create("http://" + LOCALSTACK.getHost() + ":" + LOCALSTACK.getMappedPort(4566), "us-east-1");
    }

    static StateStore store() {
        return Stores.fromEnv();
    }
}
