package io.concert.common.api;

public record TransitionRecord(
        String eventId, String eventType, String fromState, String toState, boolean accepted, long tsMillis) {}
