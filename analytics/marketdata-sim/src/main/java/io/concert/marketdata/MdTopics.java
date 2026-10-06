package io.concert.marketdata;

import java.util.List;
import java.util.Map;

/**
 * The Kafka topics of the simulator. Reference topics are compacted (key = id, the latest record per
 * id is the current version); market data topics are time-retained.
 */
public final class MdTopics {
    private MdTopics() {}

    /** @param configs topic-level configs passed to {@code AdminClient.createTopics} */
    public record TopicSpec(String name, int partitions, Map<String, String> configs) {}

    public static final String INSTRUMENTS = "ref.instruments";
    public static final String ACCOUNTS = "ref.accounts";
    public static final String VENUES = "ref.venues";
    public static final String TICKS = "md.ticks";
    public static final String BARS_1M = "md.bars.1m";

    private static final Map<String, String> COMPACT = Map.of("cleanup.policy", "compact");

    public static List<TopicSpec> specs(boolean bars) {
        TopicSpec ticks = new TopicSpec(TICKS, 6, Map.of(
                "cleanup.policy", "delete",
                "retention.ms", "3600000",
                // Retention only drops whole segments: roll every 10 min so 1 h retention actually applies.
                "segment.ms", "600000"));
        List<TopicSpec> ref = List.of(new TopicSpec(INSTRUMENTS, 1, COMPACT), new TopicSpec(ACCOUNTS, 1, COMPACT),
                new TopicSpec(VENUES, 1, COMPACT), ticks);
        return bars ? java.util.stream.Stream.concat(ref.stream(), java.util.stream.Stream.of(new TopicSpec(BARS_1M, 3,
                Map.of("cleanup.policy", "delete", "retention.ms", "86400000")))).toList() : ref;
    }
}
