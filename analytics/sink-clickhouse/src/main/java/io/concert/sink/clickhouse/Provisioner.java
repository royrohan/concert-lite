package io.concert.sink.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies a list of {@link Ddl} objects idempotently, in this order:
 *
 * <ol>
 *   <li>tables: {@code CREATE TABLE IF NOT EXISTS}, then {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS}
 *       for every column (additive schema evolution);
 *   <li>views: {@code CREATE OR REPLACE VIEW};
 *   <li>materialized views over tables (e.g. {@code mv_orders} over {@code entity_snapshots}): recreated
 *       when their definition hash (kept in the comment) changed or their target was just created, then
 *       backfilled from the source;
 *   <li>Kafka queues and their materialized views, last, so the whole downstream chain exists before
 *       anything is consumed. A queue whose definition changed is dropped (after its views) and
 *       recreated; if any view of a queue must change, all of the queue's views are dropped first (a
 *       Kafka table with no views does not consume) and recreated in list order, data views before the
 *       error view, so no record reaches only some of them.
 * </ol>
 *
 * Finally the sample queries are rewritten. Running it again with the same models changes nothing.
 */
public final class Provisioner {
    private static final Logger log = LoggerFactory.getLogger(Provisioner.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** What a run did, for logs and tests. */
    public record Report(List<String> created, List<String> recreated, List<String> backfilled) {}

    private final ClickHouseHttp ch;

    public Provisioner(ClickHouseHttp ch) {
        this.ch = ch;
    }

    public Report apply(List<Ddl> objects, List<ClickHouseSamples.Sample> samples) {
        List<String> created = new ArrayList<>();
        List<String> recreated = new ArrayList<>();
        List<String> backfilled = new ArrayList<>();
        Map<String, String> comments = existing();
        Set<String> newTables = new HashSet<>();

        for (Ddl d : objects) {
            if (d instanceof Ddl.Table t) {
                if (!comments.containsKey(t.name())) {
                    ch.execute(t.create());
                    newTables.add(t.name());
                    created.add(t.name());
                } else {
                    ch.execute(t.addColumns());
                }
            }
        }
        for (Ddl d : objects) {
            if (d instanceof Ddl.View v) {
                ch.execute(v.create());
            }
        }
        Set<String> queues = new HashSet<>();
        objects.forEach(d -> {
            if (d instanceof Ddl.Queue q) {
                queues.add(q.name());
            }
        });
        // materialized views over ordinary tables
        for (Ddl d : objects) {
            if (d instanceof Ddl.MaterializedView mv && !queues.contains(mv.source())) {
                String want = Ddl.HASH_PREFIX + mv.hash();
                boolean exists = comments.containsKey(mv.name());
                if (!exists || !want.equals(comments.get(mv.name())) || newTables.contains(mv.target())) {
                    ch.execute("DROP VIEW IF EXISTS " + Sql.id(mv.name()) + " SYNC");
                    ch.execute(mv.create());
                    (exists ? recreated : created).add(mv.name());
                    if (mv.backfill() != null) {
                        ch.execute(mv.backfill());
                        backfilled.add(mv.target());
                    }
                }
            }
        }
        // queues and their views
        Map<String, List<Ddl.MaterializedView>> viewsOf = new LinkedHashMap<>();
        for (Ddl d : objects) {
            if (d instanceof Ddl.MaterializedView mv && queues.contains(mv.source())) {
                viewsOf.computeIfAbsent(mv.source(), k -> new ArrayList<>()).add(mv);
            }
        }
        for (Ddl d : objects) {
            if (!(d instanceof Ddl.Queue q)) {
                continue;
            }
            List<Ddl.MaterializedView> views = viewsOf.getOrDefault(q.name(), List.of());
            boolean queueExists = comments.containsKey(q.name());
            boolean queueChanged = queueExists && !(Ddl.HASH_PREFIX + q.hash()).equals(comments.get(q.name()));
            boolean viewsChanged = views.stream().anyMatch(v -> !(Ddl.HASH_PREFIX + v.hash()).equals(comments.get(v.name())));
            if (queueExists && !queueChanged && !viewsChanged) {
                continue;
            }
            for (Ddl.MaterializedView v : views) {
                ch.execute("DROP VIEW IF EXISTS " + Sql.id(v.name()) + " SYNC");
            }
            if (queueChanged) {
                ch.execute("DROP TABLE IF EXISTS " + Sql.id(q.name()) + " SYNC");
                recreated.add(q.name());
            }
            if (!queueExists || queueChanged) {
                ch.execute(q.create());
                if (!queueExists) {
                    created.add(q.name());
                }
            }
            for (Ddl.MaterializedView v : views) {
                ch.execute(v.create());
                (comments.containsKey(v.name()) ? recreated : created).add(v.name());
            }
        }
        writeSamples(samples);
        Report r = new Report(created, recreated, backfilled);
        log.info("provisioned: created {}, recreated {}, backfilled {}", created, recreated, backfilled);
        return r;
    }

    /** Name to comment of every table and view in the current database. */
    Map<String, String> existing() {
        Map<String, String> out = new HashMap<>();
        for (JsonNode row : ch.select("SELECT name, comment FROM system.tables WHERE database = currentDatabase()")) {
            out.put(row.path("name").asText(), row.path("comment").asText());
        }
        return out;
    }

    private void writeSamples(List<ClickHouseSamples.Sample> samples) {
        ch.execute("TRUNCATE TABLE IF EXISTS " + ClickHouseSchema.SAMPLES);
        if (samples.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder("INSERT INTO " + ClickHouseSchema.SAMPLES + " (ord, title, sql) FORMAT JSONEachRow\n");
        for (int i = 0; i < samples.size(); i++) {
            ObjectNode n = JSON.createObjectNode();
            n.put("ord", i);
            n.put("title", samples.get(i).title());
            n.put("sql", samples.get(i).sql());
            sb.append(n).append('\n');
        }
        ch.execute(sb.toString());
    }
}
