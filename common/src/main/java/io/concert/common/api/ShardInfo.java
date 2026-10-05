package io.concert.common.api;

public record ShardInfo(String shardId, String parentShardId, String adjacentParentShardId) {}
