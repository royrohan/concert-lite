package io.concert.common.api;

import io.temporal.activity.ActivityInterface;
import java.util.List;

@ActivityInterface
public interface ShardConsumerActivities {

    List<ShardInfo> listShards(String stream);

    /**
     * Long-running, heartbeating. Reads the shard from its checkpoint, dedupes and dispatches every
     * record, and returns when the shard closes or the run budget is used up.
     */
    ShardResult consumeShard(String stream, String shardId, String initialPosition, int runMinutes);
}
