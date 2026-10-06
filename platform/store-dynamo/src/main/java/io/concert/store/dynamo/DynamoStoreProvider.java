package io.concert.store.dynamo;

import io.concert.common.Env;
import io.concert.store.StateStore;
import io.concert.store.StateStoreProvider;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * {@code STORE_KIND=dynamo}.
 *
 * <pre>
 * DYNAMO_ENDPOINT=http://localhost:4566   LocalStack; unset for AWS (default credentials chain)
 * DYNAMO_TABLE_PREFIX=concert_            table names: concert_processed_event, ...
 * DEDUPE_TTL_HOURS=24                     native TTL on dedupe rows
 * AWS_REGION=us-east-1
 * </pre>
 */
public final class DynamoStoreProvider implements StateStoreProvider {

    @Override
    public Set<String> kinds() {
        return Set.of("dynamo", "dynamodb");
    }

    @Override
    public StateStore create(String kind) {
        String endpoint = Env.get("DYNAMO_ENDPOINT", "");
        DynamoDbClientBuilder b = DynamoDbClient.builder()
                .region(Region.of(Env.get("AWS_REGION", "us-east-1")))
                .httpClientBuilder(Apache5HttpClient.builder()
                        .maxConnections(Env.getInt("STORE_POOL_SIZE", 128))
                        .connectionTimeout(Duration.ofSeconds(2)));
        if (!endpoint.isBlank()) {
            b.endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
        } else {
            b.credentialsProvider(DefaultCredentialsProvider.builder().build());
        }
        DynamoStateStore store = new DynamoStateStore(b.build(), Env.get("DYNAMO_TABLE_PREFIX", "concert_"),
                Env.getLong("DEDUPE_TTL_HOURS", 24));
        if (Env.getBool("STORE_INIT_SCHEMA", true)) {
            store.createTables();
        }
        return store;
    }
}
