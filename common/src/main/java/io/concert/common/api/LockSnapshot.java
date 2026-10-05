package io.concert.common.api;

import java.util.List;

/**
 * @param holder event id at the head of the queue (null when free)
 * @param holderState {@code dispatching} or {@code forwarded to <next key>}
 */
public record LockSnapshot(String lockKey, String holder, String holderState, List<String> waiting, long opsThisRun) {}
