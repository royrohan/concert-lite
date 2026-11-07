package io.concert.sink;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * The snapshot topics: the legacy shared {@value SnapshotCodec#TOPIC} and one {@code snapshots.<flow>} per flow
 * ({@code io.concert.common.Flows}). Kafka auto-creation is off, so a flow's worker ensures its topic on first
 * publish and the trace UI's Start / Delete flow creates and deletes it. Sinks subscribe to {@link #PATTERN}, so a new
 * flow's topic is picked up without restarting them.
 */
public final class SnapshotTopics {
    private SnapshotTopics() {}

    /** Every snapshot topic: the shared one and each flow's. */
    public static final Pattern PATTERN = Pattern.compile(Pattern.quote(SnapshotCodec.TOPIC) + "|snapshots\\..+");

    /** A consumer topic of {@code re:<regex>} subscribes to the pattern ({@link SnapshotConsumerLoop}). */
    public static final String REGEX = "re:";

    /** The consumer topic of every snapshot topic. */
    public static final String ALL = REGEX + PATTERN.pattern();

    public static final int PARTITIONS = 3;

    /** Creates a compacted snapshot topic if missing; returns true if it was created. */
    public static boolean ensure(String bootstrap, String topic) {
        try (Admin admin = admin(bootstrap)) {
            admin.createTopics(List.of(new NewTopic(topic, Optional.of(PARTITIONS), Optional.empty())
                    .configs(Map.of("cleanup.policy", "compact", "min.compaction.lag.ms", "60000")))).all()
                    .get(15, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return false;
            }
            throw new IllegalStateException("creating topic " + topic + ": " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("creating topic " + topic + ": Kafka did not answer in 15s", e);
        }
    }

    /** Deletes a topic; returns false if it did not exist. */
    public static boolean delete(String bootstrap, String topic) {
        try (Admin admin = admin(bootstrap)) {
            admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new IllegalStateException("deleting topic " + topic + ": " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("deleting topic " + topic + ": Kafka did not answer in 15s", e);
        }
    }

    private static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000, AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000));
    }
}
