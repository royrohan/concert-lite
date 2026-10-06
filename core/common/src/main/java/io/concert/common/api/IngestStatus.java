package io.concert.common.api;

import java.util.List;

public record IngestStatus(String stream, List<String> activeShards, List<String> consumedShards, long consumerRestarts) {}
