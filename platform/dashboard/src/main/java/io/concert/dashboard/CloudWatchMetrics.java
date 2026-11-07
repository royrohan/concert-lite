package io.concert.dashboard;

import io.concert.common.Env;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;

/**
 * Publishes each machine flow's numbers to CloudWatch every {@code CLOUDWATCH_PUBLISH_SEC} (default 30 s) and reads
 * them back for the machine page's Metrics tab.
 *
 * <p>Namespace {@code Concert} (override: {@code CLOUDWATCH_NAMESPACE}), dimensions {@code Flow} (the ecosystem) and
 * {@code Machine} (the smType, or {@code ev:<domain>}). Counts ({@code EventsProcessed}, {@code EventsFailed}, ...) are
 * per interval, read with {@code Sum}; latencies are per-interval percentiles in milliseconds ({@code EventLatencyP95},
 * {@code InstanceLifetimeP95}, ...) published as statistic sets, read with {@code Average} (p50) or {@code Maximum}.
 * Calls go through {@link CloudWatchApi} (the Query protocol), which LocalStack parses.
 * The numbers are the ones the dashboard derives from {@code event_trace} ({@link FlowMetrics#samples}), so every
 * runtime (Java or Rust coordinator, any worker) is covered without instrumenting it. Intervals shorter than a minute
 * are published as high-resolution metrics.
 *
 * <p>{@code CLOUDWATCH_ENDPOINT} points at LocalStack locally; unset it for real AWS (default credentials chain).
 */
final class CloudWatchMetrics implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CloudWatchMetrics.class);

    static final String NAMESPACE = Env.get("CLOUDWATCH_NAMESPACE", "Concert");
    static final int PUBLISH_SEC = Math.max(10, Env.getInt("CLOUDWATCH_PUBLISH_SEC", 30));
    /** An instance's lifetime is measured when it starts and finishes within this window. */
    static final int LIFETIME_LOOKBACK_SEC = 900;

    /** What the Metrics tab charts, in order: {name, unit, statistic, label}. */
    static final List<String[]> CHARTS = List.of(
            new String[] {"EventsProcessed", "Count", "Sum", "Events processed"},
            new String[] {"Completions", "Count", "Sum", "Completions (new → terminal)"},
            new String[] {"EventsFailed", "Count", "Sum", "Events failed"},
            new String[] {"EventsRejected", "Count", "Sum", "Events rejected"},
            new String[] {"EventLatencyP50", "Milliseconds", "Average", "Event latency p50"},
            new String[] {"EventLatencyP95", "Milliseconds", "Maximum", "Event latency p95"},
            new String[] {"EventLatencyP99", "Milliseconds", "Maximum", "Event latency p99"},
            new String[] {"InstanceLifetimeP50", "Milliseconds", "Average", "Instance lifetime p50"},
            new String[] {"InstanceLifetimeP95", "Milliseconds", "Maximum", "Instance lifetime p95"},
            new String[] {"KinesisTransitP95", "Milliseconds", "Maximum", "Kinesis transit p95"},
            new String[] {"LockWaits", "Count", "Sum", "Lock waits"},
            new String[] {"DuplicatesDropped", "Count", "Sum", "Duplicates dropped"});

    private final CloudWatchApi cw;
    private final FlowMetrics metrics;
    private final Function<String, String> flowOf;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final Thread publisher;
    private volatile boolean running = true;
    private volatile String lastError;
    private volatile long lastPublished;
    private volatile int lastDatums;

    /** @param flowOf the ecosystem of a machine flow ({@code ""} when it has none) */
    CloudWatchMetrics(CloudWatchApi cw, FlowMetrics metrics, Function<String, String> flowOf) {
        this.cw = cw;
        this.metrics = metrics;
        this.flowOf = flowOf;
        this.publisher = Env.get("CLOUDWATCH_PUBLISH", "true").equals("false") ? null
                : Thread.ofVirtual().name("cloudwatch-metrics").start(this::loop);
    }

    // ---------------------------------------------------------------- clients

    static String endpoint() {
        String e = Env.get("CLOUDWATCH_ENDPOINT", Env.get("KINESIS_ENDPOINT", ""));
        return e.isBlank() ? null : e;
    }

    private static AwsCredentialsProvider credentials() {
        return endpoint() != null ? StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
                : DefaultCredentialsProvider.builder().build();
    }

    static CloudWatchApi client() {
        return new CloudWatchApi(endpoint(), Env.get("AWS_REGION", "us-east-1"), credentials());
    }

    static CloudWatchLogsClient logsClient() {
        CloudWatchLogsClientBuilder b = CloudWatchLogsClient.builder().region(Region.of(Env.get("AWS_REGION", "us-east-1")))
                .credentialsProvider(credentials());
        if (endpoint() != null) {
            b.endpointOverride(URI.create(endpoint()));
        }
        return b.build();
    }

    // ---------------------------------------------------------------- publishing

    private void loop() {
        while (running) {
            try {
                Thread.sleep(PUBLISH_SEC * 1000L);
                publishOnce();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                if (!String.valueOf(e.getMessage()).equals(lastError)) {
                    log.warn("CloudWatch publish failed: {}", e.getMessage());
                }
                lastError = String.valueOf(e.getMessage());
            }
        }
    }

    /** One interval: every machine flow with trace rows in it. */
    void publishOnce() {
        Instant now = Instant.now();
        int resolution = PUBLISH_SEC < 60 ? 1 : 60;
        List<CloudWatchApi.Datum> datums = new ArrayList<>();
        for (FlowMetrics.MachineSample s : metrics.samples(PUBLISH_SEC, LIFETIME_LOOKBACK_SEC)) {
            Map<String, String> dims = dimensions(s.machine());
            s.counts().forEach((name, v) -> datums.add(new CloudWatchApi.Datum(name, dims, now, "Count", (double) v, null, resolution)));
            s.millis().forEach((name, v) -> datums.add(new CloudWatchApi.Datum(name, dims, now, "Milliseconds", null,
                    new double[] {1, v, v, v}, resolution)));
        }
        for (int i = 0; i < datums.size(); i += 500) { // PutMetricData takes at most 1000 datums (1 MB)
            cw.putMetricData(NAMESPACE, datums.subList(i, Math.min(datums.size(), i + 500)));
        }
        lastPublished = now.toEpochMilli();
        lastDatums = datums.size();
        if (lastError != null) {
            log.info("CloudWatch publishing again ({} datums)", datums.size());
        }
        lastError = null;
    }

    private Map<String, String> dimensions(String machine) {
        String flow = flowOf.apply(machine);
        Map<String, String> m = new LinkedHashMap<>();
        m.put("Flow", flow == null || flow.isEmpty() ? "other" : flow);
        m.put("Machine", machine);
        return m;
    }

    // ---------------------------------------------------------------- reading back

    /** Period for a window: high resolution for short windows, at most ~180 points. */
    static int period(int windowSec) {
        if (windowSec <= 900 && PUBLISH_SEC < 60) {
            return PUBLISH_SEC <= 10 ? 10 : 30;
        }
        int p = Math.max(60, windowSec / 180);
        return (p + 59) / 60 * 60;
    }

    /**
     * {@code {namespace, dimensions, period, windowSec, publishSec, lastPublished, error, charts: [{name, label, unit, stat,
     * points: [[epochMillis, value], ...]}]}} for one machine flow.
     */
    Map<String, Object> query(String machine, int windowSec) {
        Map<String, Object> out = new LinkedHashMap<>();
        int period = period(windowSec);
        Map<String, String> dims = dimensions(machine);
        out.put("namespace", NAMESPACE);
        out.put("dimensions", dims.entrySet().stream().map(d -> d.getKey() + "=" + d.getValue()).toList());
        out.put("period", period);
        out.put("windowSec", windowSec);
        out.put("publishSec", PUBLISH_SEC);
        out.put("publishing", publisher != null);
        out.put("lastPublished", lastPublished == 0 ? null : lastPublished);
        out.put("publishError", lastError);
        List<CloudWatchApi.Query> queries = new ArrayList<>();
        for (int i = 0; i < CHARTS.size(); i++) {
            String[] c = CHARTS.get(i);
            queries.add(new CloudWatchApi.Query("m" + i, NAMESPACE, c[0], dims, period, c[2]));
        }
        Instant end = Instant.now();
        Map<String, List<double[]>> points = Map.of();
        try {
            points = cw.getMetricData(queries, end.minusSeconds(windowSec), end);
        } catch (RuntimeException e) {
            out.put("error", e.getMessage());
        }
        List<Map<String, Object>> charts = new ArrayList<>();
        for (int i = 0; i < CHARTS.size(); i++) {
            String[] c = CHARTS.get(i);
            List<double[]> xs = new ArrayList<>(points.getOrDefault("m" + i, List.of()));
            xs.sort((a, b) -> Double.compare(a[0], b[0]));
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", c[0]);
            m.put("label", c[3]);
            m.put("unit", c[1]);
            m.put("stat", c[2]);
            m.put("points", xs.stream().map(p -> List.of((long) p[0], p[1])).toList());
            charts.add(m);
        }
        out.put("charts", charts);
        return out;
    }

    /**
     * LocalStack's view of CloudWatch: {@code {cloudwatch, logs}} states from {@code /_localstack/health}
     * (running, available, disabled), {@code unreachable} when it doesn't answer, {@code aws} without an endpoint.
     */
    private volatile Object[] statusCache;

    Map<String, String> status() {
        Object[] c = statusCache;
        if (c != null && System.currentTimeMillis() - (long) c[0] < 5000) {
            @SuppressWarnings("unchecked")
            Map<String, String> m = (Map<String, String>) c[1];
            return m;
        }
        Map<String, String> m = probe();
        statusCache = new Object[] {System.currentTimeMillis(), m};
        return m;
    }

    private Map<String, String> probe() {
        String e = endpoint();
        if (e == null) {
            return Map.of("cloudwatch", "aws", "logs", "aws");
        }
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(e.replaceAll("/+$", "") + "/_localstack/health"))
                    .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
            var services = io.concert.common.Json.MAPPER.readTree(r.body()).path("services");
            return Map.of("cloudwatch", services.path("cloudwatch").asText("disabled"), "logs", services.path("logs").asText("disabled"));
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Map.of("cloudwatch", "unreachable", "logs", "unreachable");
        }
    }

    @Override
    public void close() {
        running = false;
        if (publisher != null) {
            publisher.interrupt();
        }
    }
}
