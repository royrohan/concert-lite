package io.concert.common.api;

import java.util.List;

/**
 * Carried across continue-as-new.
 *
 * @param headForwardedAtMillis non-zero when the head was already forwarded to its next lock and is
 *     waiting for {@code release}
 * @param recentRequestIds bounded window of finished request ids, to reject replayed acquires
 * @param idleSeconds how long an empty lock stays open; longer means fewer cold starts for
 *     recurring keys (a cold first event costs ~5x a warm one) at the price of more open workflows
 */
public record LockState(
        String lockKey, List<LockRequest> queue, long headForwardedAtMillis, List<String> recentRequestIds, int idleSeconds) {

    public static LockState fresh(String lockKey, int idleSeconds) {
        return new LockState(lockKey, List.of(), 0, List.of(), idleSeconds);
    }
}
