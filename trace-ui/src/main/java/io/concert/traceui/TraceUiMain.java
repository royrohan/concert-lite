package io.concert.traceui;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Env;
import io.concert.common.Json;
import io.concert.common.TemporalClients;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntitySnapshot;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.samples.SampleMachines;
import io.concert.sdk.StateMachineRegistry;
import io.concert.sdk.StateMachineSpec;
import io.concert.store.JdbcStateStore;
import io.concert.store.Stores;
import io.temporal.client.WorkflowClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event tracing UI at http://localhost:8088 (TRACE_UI_PORT). Reads DSQL {@code event_trace} /
 * {@code processed_event} / {@code sm_state} and queries live workflows in Temporal.
 *
 * <pre>
 * GET /api/recent                 latest finished / dropped events
 * GET /api/events/{eventId}       timeline of one event
 * GET /api/entities/{smType:key}  live state, history, Mermaid diagram
 * GET /api/locks/{lockKey}        live holder and queue of a lock
 * </pre>
 */
public final class TraceUiMain {
    private static final Logger log = LoggerFactory.getLogger(TraceUiMain.class);

    private final JdbcStateStore store;
    private final WorkflowClient client;
    private final String temporalUi = Env.get("TEMPORAL_UI_URL", "http://localhost:8080");

    TraceUiMain(JdbcStateStore store, WorkflowClient client) {
        this.store = store;
        this.client = client;
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("STORE_INIT_SCHEMA", Env.get("STORE_INIT_SCHEMA", "false"));
        SampleMachines.registerSpecs();
        TraceUiMain ui = new TraceUiMain(Stores.fromEnv(), TemporalClients.fromEnv());
        int port = Env.getInt("TRACE_UI_PORT", 8088);
        ui.start(port);
        log.info("trace UI on http://localhost:{}", port);
    }

    HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/recent", ex -> json(ex, Map.of("events", store.recentEvents(50))));
        server.createContext("/api/events/", ex -> json(ex, event(tail(ex, "/api/events/"))));
        server.createContext("/api/entities/", ex -> json(ex, entity(tail(ex, "/api/entities/"))));
        server.createContext("/api/locks/", ex -> json(ex, lock(tail(ex, "/api/locks/"))));
        server.createContext("/", this::staticPage);
        server.start();
        return server;
    }

    Map<String, Object> event(String eventId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("eventId", eventId);
        out.put("dedupeStatus", store.processedStatus(eventId).orElse(null));
        out.put("trace", store.traceForEvent(eventId));
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows?query="
                + URLEncoder.encode("EventId=\"" + eventId + "\"", StandardCharsets.UTF_8));
        return out;
    }

    Map<String, Object> entity(String workflowId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("workflowId", workflowId);
        String smType = workflowId.contains(":") ? workflowId.substring(0, workflowId.indexOf(':')) : workflowId;
        String state = null;
        try {
            EntitySnapshot snap = client.newWorkflowStub(EntityWorkflow.class, workflowId).snapshot();
            out.put("live", snap);
            state = snap.state();
        } catch (RuntimeException notRunning) {
            out.put("live", null);
        }
        store.loadState(workflowId).ifPresent(row -> out.put("projection", row));
        if (state == null && out.get("projection") instanceof io.concert.common.api.SmStateRow row) {
            state = row.state();
        }
        StateMachineSpec spec = StateMachineRegistry.get(smType);
        out.put("mermaid", spec == null ? null : spec.toMermaid(state));
        out.put("history", store.traceForWorkflow(workflowId, 100));
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows/" + workflowId);
        return out;
    }

    Map<String, Object> lock(String lockKey) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lockKey", lockKey);
        try {
            out.put("live", client.newWorkflowStub(KeyLockWorkflow.class, WorkflowIds.lock(lockKey)).snapshot());
        } catch (RuntimeException idle) {
            out.put("live", null); // no open lock workflow: the key is free
        }
        out.put("temporalUi", temporalUi + "/namespaces/default/workflows/" + WorkflowIds.lock(lockKey));
        return out;
    }

    private static String tail(HttpExchange ex, String prefix) {
        return URLDecoder.decode(ex.getRequestURI().getRawPath().substring(prefix.length()), StandardCharsets.UTF_8);
    }

    private static void json(HttpExchange ex, Object body) throws IOException {
        byte[] bytes;
        int status = 200;
        try {
            bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            status = 500;
            bytes = Json.write(Map.of("error", String.valueOf(e.getMessage()))).getBytes(StandardCharsets.UTF_8);
        }
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    private void staticPage(HttpExchange ex) throws IOException {
        try (InputStream in = TraceUiMain.class.getResourceAsStream("/web/index.html")) {
            byte[] bytes = in.readAllBytes();
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
        }
        ex.close();
    }
}
