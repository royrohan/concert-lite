package io.concert.store.spanner;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.concert.common.Env;
import io.concert.store.StateStore;
import io.concert.store.StateStoreProvider;
import java.util.Set;

/**
 * {@code STORE_KIND=spanner}.
 *
 * <pre>
 * SPANNER_EMULATOR_HOST=localhost:9010   emulator; unset for Cloud Spanner (application default credentials)
 * SPANNER_PROJECT=concert-local   SPANNER_INSTANCE=concert   SPANNER_DATABASE=concert
 * </pre>
 *
 * With STORE_INIT_SCHEMA=true (default) it creates the database and tables if missing; against the
 * emulator it also creates the instance.
 */
public final class SpannerStoreProvider implements StateStoreProvider {

    @Override
    public Set<String> kinds() {
        return Set.of("spanner");
    }

    @Override
    public StateStore create(String kind) {
        String project = Env.get("SPANNER_PROJECT", "concert-local");
        String emulator = Env.get("SPANNER_EMULATOR_HOST", "");
        SpannerOptions.Builder b = SpannerOptions.newBuilder().setProjectId(project);
        if (!emulator.isBlank()) {
            b.setEmulatorHost(emulator);
        }
        Spanner spanner = b.build().getService();
        DatabaseId db = DatabaseId.of(project, Env.get("SPANNER_INSTANCE", "concert"), Env.get("SPANNER_DATABASE", "concert"));
        if (Env.getBool("STORE_INIT_SCHEMA", true)) {
            SpannerSchema.ensure(spanner, db, !emulator.isBlank());
        }
        return new SpannerStateStore(spanner, db);
    }
}
