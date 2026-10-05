package io.concert.orchestration;

import io.concert.common.Env;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;

/** KINESIS_ENDPOINT points at LocalStack locally; leave it unset for real AWS. */
public final class KinesisClients {
    private KinesisClients() {}

    public static KinesisClient fromEnv() {
        String endpoint = Env.get("KINESIS_ENDPOINT", "");
        return create(endpoint.isBlank() ? null : endpoint, Env.get("AWS_REGION", "us-east-1"));
    }

    public static KinesisClient create(String endpoint, String region) {
        KinesisClientBuilder b = KinesisClient.builder().region(Region.of(region));
        if (endpoint != null) {
            b.endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
        } else {
            b.credentialsProvider(DefaultCredentialsProvider.builder().build());
        }
        return b.build();
    }
}
