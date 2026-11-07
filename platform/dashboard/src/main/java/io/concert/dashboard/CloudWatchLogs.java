package io.concert.dashboard;

import io.concert.common.Env;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

/**
 * Ships the containers of every flow (and of the shared services) to CloudWatch Logs, and reads them back for the
 * machine page's Logs tab.
 *
 * <p>Log group {@code /concert/<flow>} per flow ({@code /concert/shared} for Kinesis, Kafka, the sinks, ... only with
 * {@code CLOUDWATCH_LOGS_SHARED=true}), one log
 * stream per compose service ({@code coordinator}, {@code worker}, ...): the names an ECS task with the
 * {@code awslogs} driver would use, so the Logs tab reads the same in AWS. Every {@code CLOUDWATCH_LOGS_POLL_SEC}
 * (default 15 s) it reads at most {@code CLOUDWATCH_LOGS_MAX_LINES} (default 1000, the newest) of what each running container logged since the last event shipped ({@code docker logs
 * --since}); on start it resumes after the stream's last event (at most {@code CLOUDWATCH_LOGS_BACKFILL_SEC}, default
 * 600 s, back), so a dashboard restart neither loses nor repeats much. Groups are created with a 7-day retention.
 *
 * <p>Uses the Docker CLI like {@link DockerCompose} (the dashboard has the host's Docker socket): local development.
 * {@code CLOUDWATCH_LOGS_SHIP=false} turns shipping off (reading still works).
 */
final class CloudWatchLogs implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CloudWatchLogs.class);

    static final String SHARED_GROUP = "/concert/shared";
    private static final int POLL_SEC = Math.max(1, Env.getInt("CLOUDWATCH_LOGS_POLL_SEC", 15));
    /** At most this many lines per container per poll: a container stuck in an error loop can't flood LocalStack. */
    private static final int MAX_LINES = Math.max(10, Env.getInt("CLOUDWATCH_LOGS_MAX_LINES", 1000));
    /** Also ship the shared services (Kinesis, Kafka, ClickHouse, ...) to {@link #SHARED_GROUP}. */
    private static final boolean SHIP_SHARED = Env.get("CLOUDWATCH_LOGS_SHARED", "false").equals("true");
    private static final long BACKFILL_MS = Env.getInt("CLOUDWATCH_LOGS_BACKFILL_SEC", 600) * 1000L;
    private static final int MAX_MESSAGE = 200_000; // CloudWatch: 256 KB per event including overhead
    private static final int MAX_BATCH = 5_000;

    /** A log stream to fill: group, stream (service) and container. */
    record Target(String group, String stream, String container) {}

    private final CloudWatchLogsClient logs;
    private final DockerCompose docker;
    private final Supplier<List<FlowRegistry.Flow>> flows;
    /** Last shipped event per group/stream, epoch nanos. */
    private final Map<String, Long> shippedUntil = new HashMap<>();
    private final Set<String> created = new HashSet<>();
    private final Thread shipper;
    private volatile boolean running = true;
    private volatile String lastError;
    private volatile long lastShipped;
    private volatile long eventsShipped;
    private volatile long droppedLines;

    CloudWatchLogs(CloudWatchLogsClient logs, DockerCompose docker, Supplier<List<FlowRegistry.Flow>> flows) {
        this.logs = logs;
        this.docker = docker;
        this.flows = flows;
        this.shipper = Env.get("CLOUDWATCH_LOGS_SHIP", "true").equals("false") || docker.unavailable() != null ? null
                : Thread.ofVirtual().name("cloudwatch-logs").start(this::loop);
    }

    static String group(String flow) {
        return "/concert/" + flow;
    }

    // ---------------------------------------------------------------- shipping

    private void loop() {
        while (running) {
            try {
                Thread.sleep(POLL_SEC * 1000L);
                shipOnce();
                if (lastError != null) {
                    log.info("CloudWatch Logs shipping again");
                }
                lastError = null;
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                if (!String.valueOf(e.getMessage()).equals(lastError)) {
                    log.warn("CloudWatch Logs shipping failed: {}", e.getMessage());
                }
                lastError = String.valueOf(e.getMessage());
            }
        }
    }

    /** The running containers to ship: each flow's services, then the shared ones. */
    List<Target> targets() {
        Map<String, Map<String, String>> containers = docker.containers();
        List<Target> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (FlowRegistry.Flow f : flows.get()) {
            for (String service : f.services()) {
                add(out, seen, containers, group(f.id()), service);
            }
        }
        if (SHIP_SHARED) {
            for (String[] s : FlowOps.SHARED) {
                add(out, seen, containers, SHARED_GROUP, s[1]);
            }
        }
        return out;
    }

    private static void add(List<Target> out, Set<String> seen, Map<String, Map<String, String>> containers, String group,
            String service) {
        Map<String, String> c = containers.get(service);
        if (c != null && "running".equals(c.get("state")) && c.get("name") != null && !c.get("name").isEmpty() && seen.add(service)) {
            out.add(new Target(group, service, c.get("name")));
        }
    }

    void shipOnce() {
        for (Target t : targets()) {
            ship(t);
        }
    }

    private void ship(Target t) {
        String key = t.group() + "|" + t.stream();
        ensureStream(t.group(), t.stream());
        long since = shippedUntil.computeIfAbsent(key, k -> resumePoint(t.group(), t.stream()));
        List<InputLogEvent> events = new ArrayList<>();
        long[] last = {since};
        int rc = docker.logsSince(t.container(), since, line -> {
            int sp = line.indexOf(' ');
            if (sp < 20) {
                return; // not a "<timestamp> <message>" line (a docker CLI error)
            }
            try {
                Instant ts = Instant.parse(line.substring(0, sp));
                long nanos = ts.getEpochSecond() * 1_000_000_000L + ts.getNano();
                if (nanos <= since) {
                    return; // --since is inclusive
                }
                String msg = line.substring(sp + 1);
                events.add(InputLogEvent.builder().timestamp(ts.toEpochMilli())
                        .message(msg.isEmpty() ? " " : msg.length() > MAX_MESSAGE ? msg.substring(0, MAX_MESSAGE) : msg).build());
                last[0] = Math.max(last[0], nanos);
            } catch (DateTimeParseException e) {
                // a continuation without a timestamp: Docker prefixes every line, so this is CLI output
            }
        }, 30);
        if (rc != 0 && events.isEmpty()) {
            return; // container gone between ps and logs
        }
        events.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));
        if (events.size() > MAX_LINES) { // keep the newest, and say how much was left out
            int dropped = events.size() - MAX_LINES;
            long at = events.get(dropped).timestamp();
            events.subList(0, dropped).clear();
            events.add(0, InputLogEvent.builder().timestamp(at)
                    .message("[cloudwatch-logs] " + dropped + " older lines from " + t.stream() + " not shipped (more than "
                            + MAX_LINES + " in " + POLL_SEC + " s; CLOUDWATCH_LOGS_MAX_LINES)").build());
            droppedLines += dropped;
        }
        for (int i = 0; i < events.size(); i += MAX_BATCH) {
            List<InputLogEvent> chunk = events.subList(i, Math.min(events.size(), i + MAX_BATCH));
            try {
                logs.putLogEvents(r -> r.logGroupName(t.group()).logStreamName(t.stream()).logEvents(chunk));
            } catch (ResourceNotFoundException e) { // LocalStack was recreated: make the group and stream again next poll
                created.remove(t.group());
                created.remove(key);
                throw e;
            }
            eventsShipped += chunk.size();
        }
        if (!events.isEmpty()) {
            lastShipped = System.currentTimeMillis();
        }
        shippedUntil.put(key, last[0]);
    }

    /** After the stream's newest event, but at most the backfill window back. */
    private long resumePoint(String group, String stream) {
        long floor = System.currentTimeMillis() - BACKFILL_MS;
        try {
            List<OutputLogEvent> e = logs.getLogEvents(r -> r.logGroupName(group).logStreamName(stream).startFromHead(false).limit(1)).events();
            if (!e.isEmpty()) {
                floor = Math.max(floor, e.get(e.size() - 1).timestamp() + 1);
            }
        } catch (ResourceNotFoundException ignored) {
            // new stream
        }
        return floor * 1_000_000L;
    }

    private void ensureStream(String group, String stream) {
        if (created.contains(group + "|" + stream)) {
            return;
        }
        if (created.add(group)) {
            try {
                logs.createLogGroup(r -> r.logGroupName(group));
                logs.putRetentionPolicy(r -> r.logGroupName(group).retentionInDays(7));
            } catch (ResourceAlreadyExistsException ignored) {
                // exists
            } catch (RuntimeException e) {
                created.remove(group);
                throw e;
            }
        }
        try {
            logs.createLogStream(r -> r.logGroupName(group).logStreamName(stream));
        } catch (ResourceAlreadyExistsException ignored) {
            // exists
        }
        created.add(group + "|" + stream);
    }

    // ---------------------------------------------------------------- reading back

    /**
     * The newest {@code limit} events of a flow's log group in the last {@code windowSec}: {@code {group, streams,
     * events: [{ts, stream, message}], truncated, shipping, error}}, newest first.
     *
     * @param streams the log streams to read (empty: all of the group's)
     * @param filter a CloudWatch Logs filter pattern ({@code ERROR}, {@code "lock wait"}, {@code ?WARN ?ERROR}), or empty
     */
    Map<String, Object> query(String flow, int windowSec, List<String> streams, String filter, int limit) {
        String group = flow == null || flow.isEmpty() ? SHARED_GROUP : group(flow);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("group", group);
        out.put("windowSec", windowSec);
        out.put("shipping", shipper != null);
        out.put("shipError", lastError);
        out.put("lastShipped", lastShipped == 0 ? null : lastShipped);
        try {
            out.put("streams", logs.describeLogStreams(r -> r.logGroupName(group)).logStreams().stream()
                    .map(s -> s.logStreamName()).sorted().toList());
        } catch (ResourceNotFoundException e) {
            out.put("streams", List.of());
            out.put("events", List.of());
            out.put("note", "no log group " + group + " yet: logs appear once a container of this flow runs and the dashboard ships it");
            return out;
        } catch (RuntimeException e) {
            out.put("error", e.getMessage());
            out.put("streams", List.of());
            out.put("events", List.of());
            return out;
        }
        long start = System.currentTimeMillis() - windowSec * 1000L;
        Deque<Map<String, Object>> newest = new ArrayDeque<>();
        boolean truncated = false;
        try {
            String token = null;
            int pages = 0;
            do {
                String t = token;
                FilterLogEventsResponse r = logs.filterLogEvents(b -> {
                    b.logGroupName(group).startTime(start).limit(1000).nextToken(t);
                    if (streams != null && !streams.isEmpty()) {
                        b.logStreamNames(streams);
                    }
                    if (filter != null && !filter.isBlank()) {
                        b.filterPattern(filter);
                    }
                });
                for (FilteredLogEvent e : r.events()) {
                    if (!matches(filter, e.message())) {
                        continue;
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("ts", e.timestamp());
                    m.put("stream", e.logStreamName());
                    m.put("message", e.message());
                    newest.addLast(m);
                    if (newest.size() > limit) {
                        newest.removeFirst();
                        truncated = true;
                    }
                }
                token = r.nextToken();
            } while (token != null && ++pages < 50);
            truncated |= token != null;
        } catch (RuntimeException e) {
            out.put("error", e.getMessage());
        }
        List<Map<String, Object>> events = new ArrayList<>(newest);
        events.sort((a, b) -> Long.compare((long) b.get("ts"), (long) a.get("ts")));
        out.put("events", events);
        out.put("truncated", truncated);
        return out;
    }

    /**
     * CloudWatch Logs' text filter-pattern semantics, applied to what FilterLogEvents returns (LocalStack does not apply
     * the pattern; real CloudWatch already has, so this keeps everything): every term must occur, {@code "a phrase"}
     * as written, {@code ?a ?b} at least one of them, {@code -term} must not occur; case-sensitive. JSON and
     * space-delimited patterns ({@code {...}}, {@code [...]}) are left to CloudWatch.
     */
    static boolean matches(String pattern, String message) {
        if (pattern == null || pattern.isBlank()) {
            return true;
        }
        String p = pattern.strip();
        if (p.startsWith("{") || p.startsWith("[")) {
            return true;
        }
        List<String> any = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([?-]?)(\"[^\"]*\"|\\S+)").matcher(p);
        while (m.find()) {
            String op = m.group(1), term = m.group(2);
            if (term.length() >= 2 && term.startsWith("\"") && term.endsWith("\"")) {
                term = term.substring(1, term.length() - 1);
            }
            if (term.isEmpty()) {
                continue;
            }
            switch (op) {
                case "?" -> any.add(term);
                case "-" -> {
                    if (message.contains(term)) {
                        return false;
                    }
                }
                default -> {
                    if (!message.contains(term)) {
                        return false;
                    }
                }
            }
        }
        return any.isEmpty() || any.stream().anyMatch(message::contains);
    }

    Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("shipping", shipper != null);
        m.put("error", lastError);
        m.put("lastShipped", lastShipped == 0 ? null : lastShipped);
        m.put("eventsShipped", eventsShipped);
        m.put("droppedLines", droppedLines);
        m.put("pollSec", POLL_SEC);
        m.put("maxLinesPerPoll", MAX_LINES);
        m.put("shared", SHIP_SHARED);
        return m;
    }

    @Override
    public void close() {
        running = false;
        if (shipper != null) {
            shipper.interrupt();
        }
        logs.close();
    }
}
