package io.concert.sink;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * Lightweight sink metrics without a metrics library: a latency reservoir (the last
 * {@value #RESERVOIR} batch durations, for p50 / p99) and a Prometheus text-format writer
 * ({@code text/plain; version=0.0.4}).
 */
public final class SinkMetrics {

    static final int RESERVOIR = 2048;

    private final long[] nanos = new long[RESERVOIR];
    private int size;
    private int next;
    private long count;
    private long sumNanos;

    /** Records one batch apply duration. */
    public synchronized void recordBatch(long durationNanos) {
        nanos[next] = durationNanos;
        next = (next + 1) % RESERVOIR;
        size = Math.min(size + 1, RESERVOIR);
        count++;
        sumNanos += durationNanos;
    }

    /** Quantile ({@code 0..1}) of the recent batch durations in seconds; 0 if none yet. */
    public synchronized double quantileSeconds(double q) {
        if (size == 0) {
            return 0;
        }
        long[] sorted = Arrays.copyOf(nanos, size);
        Arrays.sort(sorted);
        int idx = (int) Math.min(size - 1, Math.max(0, Math.ceil(q * size) - 1));
        return sorted[idx] / 1e9;
    }

    public synchronized long batchCount() {
        return count;
    }

    public synchronized double batchSumSeconds() {
        return sumNanos / 1e9;
    }

    /** Builds a Prometheus text exposition. */
    public static final class Text {
        private final StringBuilder sb = new StringBuilder();

        /** One metric with a single sample; {@code labels} may be empty. */
        public Text metric(String name, String type, String help, Map<String, String> labels, double value) {
            header(name, type, help);
            return sample(name, labels, value);
        }

        public Text header(String name, String type, String help) {
            sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
            sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
            return this;
        }

        public Text sample(String name, Map<String, String> labels, double value) {
            sb.append(name);
            if (!labels.isEmpty()) {
                sb.append('{');
                boolean first = true;
                for (var e : labels.entrySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    sb.append(e.getKey()).append("=\"")
                            .append(e.getValue().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")).append('"');
                }
                sb.append('}');
            }
            sb.append(' ').append(format(value)).append('\n');
            return this;
        }

        private static String format(double v) {
            if (v == Math.rint(v) && Math.abs(v) < 1e15) {
                return Long.toString((long) v);
            }
            return String.format(Locale.ROOT, "%.6f", v);
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
