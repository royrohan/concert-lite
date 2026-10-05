package io.concert.common.api;

public record ShardResult(String shardId, boolean closed, String lastSequenceNumber, long recordsProcessed) {}
