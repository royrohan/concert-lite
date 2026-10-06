package io.concert.sdk.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.SmStateRow;
import io.concert.model.runtime.ModelJson;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import io.temporal.activity.Activity;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

/** The handler side of the processor: runs handlers against the StateStore and persists their results. */
public final class EventHandlerActivitiesImpl implements EventHandlerActivities {

    private final StateStore store;
    private final TraceWriter traces;
    private final HandlerRegistry handlers;

    public EventHandlerActivitiesImpl(StateStore store, TraceWriter traces, HandlerRegistry handlers) {
        this.store = store;
        this.traces = traces;
        this.handlers = handlers;
    }

    @Override
    public HandlerResult apply(EventEnvelope event) {
        int attempt = Activity.getExecutionContext().getInfo().getAttempt();
        // Durable dedupe: a DONE lifecycle row means it was applied (duplicate delivery, or a re-run after the
        // processor crashed between persisting and recording the outcome).
        Optional<SmStateRow> row = store.loadState(EventRows.eventRowId(event.eventId()));
        if (row.isPresent() && EventLifecycle.DONE.name().equals(row.get().state())) {
            return new HandlerResult(HandlerResult.Status.ALREADY_DONE, null, 0, List.of(),
                    EventRows.emitted(row.get().data()), row.get().data(), row.get().version());
        }
        HandlerRegistry.Entry entry = handlers.get(event.eventType());
        if (entry == null) {
            return HandlerResult.failed(HandlerResult.Status.NO_HANDLER,
                    "no handler for " + event.eventType() + " in domain " + handlers.domain(), attempt);
        }
        Object payload;
        try {
            payload = event.payload() == null ? null : ModelJson.read(event.payload(), entry.payloadType());
        } catch (UncheckedIOException e) {
            String detail = e.getCause() instanceof JsonProcessingException j ? j.getOriginalMessage() : e.getMessage();
            return HandlerResult.failed(HandlerResult.Status.NON_BLOCKING,
                    "invalid " + entry.payloadType().getSimpleName() + " payload: " + detail, attempt);
        }
        EventContextImpl ctx = new EventContextImpl(store, event, payload, attempt, handlers.domain());
        try {
            apply(entry.handler(), payload, ctx);
        } catch (BlockingError e) {
            return HandlerResult.failed(HandlerResult.Status.BLOCKING, e.getMessage(), attempt);
        } catch (NonBlockingError e) {
            return HandlerResult.failed(HandlerResult.Status.NON_BLOCKING, e.getMessage(), attempt);
        } catch (RuntimeException e) {
            throw e; // retried per the type's policy
        } catch (Exception e) {
            throw Activity.wrap(e);
        }
        return HandlerResult.ok(attempt, ctx.writes(), ctx.emitted());
    }

    @SuppressWarnings("unchecked")
    private static <T> void apply(EventHandler<T> handler, Object payload, EventContext ctx) throws Exception {
        handler.apply((T) payload, ctx);
    }

    @Override
    public void persist(List<SmStateRow> rows, List<TraceRow> traceRows) {
        for (SmStateRow r : rows) {
            store.upsertState(r, null);
        }
        if (traceRows != null && !traceRows.isEmpty()) {
            traces.addAll(traceRows);
        }
    }
}
