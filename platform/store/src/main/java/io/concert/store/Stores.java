package io.concert.store;

import io.concert.common.Env;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Picks the backend with {@code STORE_KIND}:
 *
 * <ul>
 *   <li>{@code postgres} (default): local Postgres 16 standing in for DSQL
 *   <li>{@code dsql}: Aurora DSQL through the AWS DSQL JDBC connector (IAM auth)
 *   <li>{@code dynamo}: DynamoDB ({@code DYNAMO_ENDPOINT} for LocalStack), module store-dynamo
 *   <li>{@code spanner}: Cloud Spanner ({@code SPANNER_EMULATOR_HOST} for the emulator), module store-spanner
 * </ul>
 */
public final class Stores {
    private Stores() {}

    public static StateStore fromEnv() {
        return create(Env.get("STORE_KIND", "postgres").toLowerCase());
    }

    public static StateStore create(String kind) {
        List<String> available = new ArrayList<>();
        for (StateStoreProvider p : ServiceLoader.load(StateStoreProvider.class)) {
            if (p.kinds().contains(kind)) {
                return p.create(kind);
            }
            available.addAll(p.kinds());
        }
        throw new IllegalArgumentException("STORE_KIND=" + kind + " has no provider on the classpath; available: " + available);
    }
}
