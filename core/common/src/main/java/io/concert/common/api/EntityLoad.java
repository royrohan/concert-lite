package io.concert.common.api;

/**
 * An entity as stored (DB-truth) and, if the event was applied before, its stored result.
 *
 * @param row the entity, or null if it has no committed transition yet
 * @param applied the result recorded when this event was first applied, or null
 */
public record EntityLoad(SmStateRow row, TransitionResult applied) {}
