package io.concert.sdk.events;

/**
 * A keyed-state document a handler saved: row {@code state:<key>}, smType {@code st_<snake stateType>}.
 *
 * @param version the stored version + 1 (the store keeps the highest version, so a repeated write is a no-op)
 */
public record StateWrite(String key, String stateType, String data, long version) {}
