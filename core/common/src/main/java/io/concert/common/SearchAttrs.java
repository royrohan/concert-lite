package io.concert.common;

import io.temporal.common.SearchAttributeKey;
import io.temporal.common.SearchAttributes;
import java.util.List;

/**
 * Custom search attributes. They must be registered on the namespace (docker-compose
 * temporal-init does it; the integration tests pass --search-attribute to the dev server). Set
 * {@code TEMPORAL_SEARCH_ATTRIBUTES=false} to run against a server without them.
 */
public final class SearchAttrs {
    private SearchAttrs() {}

    public static final SearchAttributeKey<String> EVENT_ID = SearchAttributeKey.forKeyword("EventId");
    public static final SearchAttributeKey<String> SM_TYPE = SearchAttributeKey.forKeyword("SmType");
    public static final SearchAttributeKey<String> INSTANCE_KEY = SearchAttributeKey.forKeyword("InstanceKey");
    public static final SearchAttributeKey<List<String>> LOCK_KEYS = SearchAttributeKey.forKeywordList("LockKeys");

    public static boolean enabled() {
        return Env.getBool("TEMPORAL_SEARCH_ATTRIBUTES", true);
    }

    public static SearchAttributes forEvent(EventEnvelope e) {
        if (!enabled()) {
            return SearchAttributes.EMPTY;
        }
        return SearchAttributes.newBuilder()
                .set(EVENT_ID, e.eventId())
                .set(SM_TYPE, e.smType())
                .set(INSTANCE_KEY, e.instanceKey())
                .set(LOCK_KEYS, e.effectiveLockKeys())
                .build();
    }

    public static SearchAttributes forEntity(String smType, String instanceKey) {
        if (!enabled()) {
            return SearchAttributes.EMPTY;
        }
        return SearchAttributes.newBuilder()
                .set(SM_TYPE, smType)
                .set(INSTANCE_KEY, instanceKey)
                .build();
    }

    public static SearchAttributes forLock(String lockKey) {
        if (!enabled()) {
            return SearchAttributes.EMPTY;
        }
        return SearchAttributes.newBuilder().set(LOCK_KEYS, List.of(lockKey)).build();
    }
}
