package io.concert.sdk.reconcile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Read-only view of the entity versions a sink stores. */
public interface SinkVersions {

    /**
     * Stored version per entity id (ids without a row are absent); empty if {@code smType} is not one of
     * the sink's roots (then there is nothing to reconcile).
     */
    Optional<Map<String, Long>> versions(String smType, List<String> entityIds);
}
