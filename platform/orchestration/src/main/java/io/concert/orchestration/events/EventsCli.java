package io.concert.orchestration.events;

import io.concert.common.Json;
import io.concert.common.TemporalClients;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.EventOutcome;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import io.temporal.client.WorkflowClient;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * {@code bin/events} (and {@code scripts/events.sh}): operator commands for event-style events.
 *
 * <pre>
 * events list [STATUS|all] [idPrefix]   STATUS: DONE, ERROR_BLOCKING, ERROR_NON_BLOCKING, SCHEDULED, NEW, errors (both ERROR_*)
 * events show EVENT_ID                  the lifecycle row as JSON
 * events retry EVENT_ID
 * events skip EVENT_ID [reason...]
 * events status EVENT_ID                the owning processor's live status
 * </pre>
 *
 * Uses the usual STORE_* and TEMPORAL_* environment.
 */
public final class EventsCli {
    private EventsCli() {}

    public static void main(String[] args) {
        if (args.length == 0 || !java.util.Set.of("list", "show", "retry", "skip", "status")
                .contains(args[0].toLowerCase(Locale.ROOT))) {
            usage();
            System.exit(2);
        }
        WorkflowClient client = TemporalClients.fromEnv();
        int code;
        try (StateStore store = Stores.fromEnv()) {
            code = run(new EventOps(client, store), args);
        } finally {
            client.getWorkflowServiceStubs().shutdownNow();
        }
        System.exit(code);
    }

    static int run(EventOps ops, String[] args) {
        String cmd = args[0].toLowerCase(Locale.ROOT);
        switch (cmd) {
            case "list" -> {
                String which = args.length > 1 ? args[1] : "errors";
                String prefix = args.length > 2 ? args[2] : "";
                List<EventOps.EventRow> rows;
                if (which.equalsIgnoreCase("all")) {
                    rows = ops.listByPrefix(prefix);
                } else if (which.equalsIgnoreCase("errors")) {
                    rows = new java.util.ArrayList<>(ops.list(EventLifecycle.ERROR_BLOCKING));
                    rows.addAll(ops.list(EventLifecycle.ERROR_NON_BLOCKING));
                } else {
                    rows = ops.list(EventLifecycle.valueOf(which.toUpperCase(Locale.ROOT)));
                }
                rows = rows.stream().filter(r -> r.eventId().startsWith(prefix)).toList();
                int idW = Math.max(5, rows.stream().mapToInt(r -> r.eventId().length()).max().orElse(0));
                int typeW = Math.max(4, rows.stream().mapToInt(r -> String.valueOf(r.eventType()).length()).max().orElse(0));
                int parentW = Math.max(6, rows.stream().mapToInt(r -> r.parentEventId() == null ? 1 : r.parentEventId().length()).max().orElse(0));
                String fmt = "%-" + idW + "s  %-" + typeW + "s  %-10s %-19s %-8s %-" + parentW + "s  %s%n";
                System.out.printf(fmt, "EVENT", "TYPE", "DOMAIN", "STATUS", "ATTEMPTS", "PARENT", "CHILDREN / ERROR");
                for (EventOps.EventRow r : rows) {
                    String tail = r.error() != null ? r.error() : String.join(",", r.children());
                    System.out.printf(fmt, r.eventId(), r.eventType(), r.domain(), r.status(), r.attempts(),
                            r.parentEventId() == null ? "-" : r.parentEventId(), tail);
                }
                System.out.println(rows.size() + " event(s)");
                return 0;
            }
            case "show" -> {
                need(args, 2);
                System.out.println(ops.get(args[1]).map(Json::write).orElse("no lifecycle row for " + args[1]));
                return 0;
            }
            case "retry" -> {
                need(args, 2);
                print(ops.retry(args[1]));
                return 0;
            }
            case "skip" -> {
                need(args, 2);
                String reason = args.length > 2 ? String.join(" ", List.of(args).subList(2, args.length)) : "operator skip";
                print(ops.skip(args[1], reason));
                return 0;
            }
            case "status" -> {
                need(args, 2);
                var s = ops.status(args[1]);
                System.out.println("processor evproc:" + s.domain() + ":" + s.key() + " (processed this run: "
                        + s.processedThisRun() + ")");
                s.scheduled().forEach(i -> System.out.println("  SCHEDULED  " + i.eventId() + " " + i.eventType() + " at "
                        + Instant.ofEpochMilli(i.atMillis())));
                s.blocked().forEach(i -> System.out.println("  BLOCKED    " + i.eventId() + " " + i.eventType()
                        + " attempts=" + i.attempts() + " request=" + i.requestId() + " error=" + i.error()));
                s.parked().forEach(i -> System.out.println("  PARKED     " + i.eventId() + " " + i.eventType()
                        + " attempts=" + i.attempts() + (i.requestId() != null ? " retrying=" + i.requestId() : "")
                        + " error=" + i.error()));
                s.recent().forEach(r -> System.out.println("  recent     " + r.eventId() + " " + r.eventType() + " "
                        + r.status() + (r.detail() != null ? " " + r.detail() : "")));
                return 0;
            }
            default -> {
                usage();
                return 2;
            }
        }
    }

    private static void print(EventOutcome o) {
        System.out.println(o.eventId() + " -> " + o.status() + (o.detail() != null ? " (" + o.detail() + ")" : "")
                + (o.children().isEmpty() ? "" : " children=" + o.children()));
    }

    private static void need(String[] args, int n) {
        if (args.length < n) {
            usage();
            System.exit(2);
        }
    }

    private static void usage() {
        System.err.println("""
                usage: events list [STATUS|errors|all] [idPrefix]
                       events show EVENT_ID
                       events retry EVENT_ID
                       events skip EVENT_ID [reason...]
                       events status EVENT_ID""");
    }
}
