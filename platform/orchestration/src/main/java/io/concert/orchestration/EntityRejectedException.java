package io.concert.orchestration;

/** Failure type used when the entity's update validator rejects an event; never retried. */
final class EntityRejectedException {
    private EntityRejectedException() {}

    static final String TYPE = "EntityRejected";
}
