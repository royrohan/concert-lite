package io.concert.common.api;

public record TransitionResult(
        String eventId, String fromState, String toState, boolean accepted, String message, long version) {}
