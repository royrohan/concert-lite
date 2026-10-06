package io.concert.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.concert.common.Env;
import java.util.Set;

/**
 * {@code postgres}: local Postgres standing in for DSQL. {@code dsql}: real Aurora DSQL through the
 * AWS DSQL JDBC connector, which mints IAM auth tokens; set STORE_JDBC_URL to
 * {@code jdbc:aws-dsql:postgresql://<cluster-endpoint>/postgres} and STORE_USER (e.g. admin).
 */
public final class JdbcStoreProvider implements StateStoreProvider {

    @Override
    public Set<String> kinds() {
        return Set.of("postgres", "dsql");
    }

    @Override
    public StateStore create(String kind) {
        boolean dsql = kind.equals("dsql");
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(Env.get("STORE_JDBC_URL", "jdbc:postgresql://localhost:5433/concert"));
        cfg.setUsername(Env.get("STORE_USER", dsql ? "admin" : "concert"));
        if (!dsql) {
            cfg.setPassword(Env.get("STORE_PASSWORD", "concert"));
        }
        cfg.setMaximumPoolSize(Env.getInt("STORE_POOL_SIZE", 32));
        cfg.setMinimumIdle(Math.min(4, cfg.getMaximumPoolSize()));
        // DSQL closes connections after 60 minutes; recycle well before that.
        cfg.setMaxLifetime(dsql ? 50 * 60_000L : 30 * 60_000L);
        cfg.setPoolName("concert-store");
        HikariDataSource ds = new HikariDataSource(cfg);
        if (Env.getBool("STORE_INIT_SCHEMA", true)) {
            SchemaInitializer.apply(ds, dsql);
        }
        return new JdbcStateStore(ds, kind);
    }
}
