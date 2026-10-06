package io.concert.sink;

import org.apache.kafka.common.TopicPartition;

/** A decoded snapshot and where it was read from, so targets can store their offsets with the data. */
public record SnapshotRecord(EntitySnapshot snapshot, String topic, int partition, long offset) {

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}
