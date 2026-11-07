package io.concert.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Flows;
import io.concert.common.Json;
import io.concert.common.TaskQueues;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.EventCatalogs;
import io.concert.sdk.events.EventRows;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The flows the landing page offers: one per ecosystem (catalog group: {@code samples}, {@code trading} and every
 * generated ecosystem under {@code showcases/}), each with what its page needs to run it. That means its compose
 * services (a coordinator and its workers), its own Kinesis stream, Kafka topic and orchestration queue
 * ({@link Flows}), its machine flows (the per-smType / per-domain pages of {@link FlowMetrics}) and the scope of
 * its data, for Delete flow.
 */
final class FlowRegistry {

    /**
     * One flow (ecosystem).
     *
     * @param composeFiles override file of the flow, relative to the repo ({@code null} for flows in docker-compose.yml)
     * @param machineFlows ids of its per-machine flows ({@code smType}, {@code ev:<domain>})
     * @param smTypes sm_type values of its rows: entity types, event lifecycle ({@code evt_*}) and keyed state ({@code st_*})
     * @param taskQueues its worker task queues plus its orchestration queue
     * @param workflowPrefixes ids of its workflows start with one of these (traces and Temporal clean-up)
     */
    record Flow(String id, String label, String profile, String composeFile, String coordinator, List<String> workers,
            List<String> machineFlows, List<String> machines, List<String> eventCatalogs, Set<String> smTypes,
            List<String> taskQueues, List<String> workflowPrefixes) {

        List<String> services() {
            List<String> out = new ArrayList<>();
            out.add(coordinator);
            out.addAll(workers);
            return out;
        }

        String stream() {
            return Flows.stream(id);
        }

        String topic() {
            return Flows.snapshotTopic(id);
        }

        String orchestrationQueue() {
            return Flows.orchestrationQueue(id);
        }
    }

    /** Catalog groups that belong to another flow: the demo event domain runs in the samples worker. */
    private static final Map<String, String> ALIASES = Map.of("demo", "samples");

    /** Flows defined in docker-compose.yml itself (generated ecosystems bring an ecosystem.json). */
    private static final Map<String, String[]> BUILT_IN = Map.of(
            "samples", new String[] {"Samples", "apps", "samples-coordinator", "worker"},
            "trading", new String[] {"Trading", "trading", "trading-coordinator", "trading-worker"});

    private final Map<String, Flow> flows;

    FlowRegistry(Path repo, List<MachineCatalog> machines, List<EventCatalog> events) {
        this.flows = build(repo, machines, events);
    }

    /** The flow (ecosystem) a catalog belongs to: {@code order-events/orders} and {@code order-events} to {@code order-events}. */
    static String ecosystemOf(String catalogOrGroup) {
        String g = catalogOrGroup.contains("/") ? catalogOrGroup.substring(0, catalogOrGroup.indexOf('/')) : catalogOrGroup;
        return ALIASES.getOrDefault(g, g);
    }

    List<Flow> all() {
        return List.copyOf(flows.values());
    }

    Flow get(String id) {
        Flow f = flows.get(id);
        if (f == null) {
            throw new IllegalArgumentException("unknown flow " + id + ", expected one of " + flows.keySet());
        }
        return f;
    }

    /** The flow of a machine flow ({@code smType} or {@code ev:<domain>}), or {@code null}. */
    Flow ofMachineFlow(String machineFlow) {
        return flows.values().stream().filter(f -> f.machineFlows().contains(machineFlow)).findFirst().orElse(null);
    }

    /** The ingest stream of a machine flow: its flow's, else the legacy shared stream. */
    String streamOf(String machineFlow) {
        Flow f = ofMachineFlow(machineFlow);
        return f == null ? Flows.stream("") : f.stream();
    }

    /** Every compose override of a generated ecosystem, so each compose call sees the same project. */
    List<String> composeFiles() {
        return flows.values().stream().map(Flow::composeFile).filter(java.util.Objects::nonNull).distinct().toList();
    }

    private static Map<String, Flow> build(Path repo, List<MachineCatalog> machines, List<EventCatalog> events) {
        Map<String, JsonNode> ecosystems = ecosystems(repo);
        Map<String, Builder> b = new LinkedHashMap<>();
        for (MachineCatalog c : machines) {
            Builder x = b.computeIfAbsent(ecosystemOf(c.name()), Builder::new);
            for (MachineCatalog.Machine m : c.machines()) {
                x.machineFlows.add(m.smType());
                x.machines.add(m.smType());
                x.smTypes.add(m.smType());
                x.taskQueues.add(TaskQueues.stateMachine(m.smType()));
                x.prefixes.add(m.smType() + ":");
            }
        }
        for (EventCatalog c : events.stream().sorted(Comparator.comparing(EventCatalog::name)).toList()) {
            Builder x = b.computeIfAbsent(ecosystemOf(c.name()), Builder::new);
            x.eventCatalogs.add(c.name());
            for (EventCatalog.EventType t : c.events()) {
                x.machineFlows.add("ev:" + t.domain());
                x.smTypes.add(EventRows.eventSmType(t.name()));
                x.taskQueues.add(TaskQueues.events(t.domain()));
                x.prefixes.add("evproc:" + t.domain() + ":");
            }
            c.states().forEach(s -> x.smTypes.add(EventRows.stateSmType(s.name())));
        }
        Map<String, Flow> out = new LinkedHashMap<>();
        for (Builder x : b.values()) {
            String id = x.id;
            String label;
            String profile;
            String file = null;
            String coordinator = id + "-coordinator";
            List<String> workers = new ArrayList<>();
            if (BUILT_IN.containsKey(id)) {
                String[] d = BUILT_IN.get(id);
                label = d[0];
                profile = d[1];
                coordinator = d[2];
                workers.add(d[3]);
            } else if (ecosystems.containsKey(id)) {
                JsonNode e = ecosystems.get(id);
                label = id;
                profile = e.path("profile").asText(id);
                file = e.path("module").asText("showcases/" + id) + "/compose.yml";
                if (!e.path("service").asText().isEmpty()) {
                    workers.add(e.path("service").asText());
                }
                if (!e.path("eventService").asText().isEmpty()) {
                    workers.add(e.path("eventService").asText());
                }
            } else {
                continue; // a catalog without a deployment (e.g. a test catalog on the class path)
            }
            List<String> taskQueues = new ArrayList<>(x.taskQueues);
            taskQueues.add(Flows.orchestrationQueue(id));
            List<String> prefixes = new ArrayList<>(x.prefixes);
            prefixes.add(Flows.lockWorkflowId(id, ""));
            out.put(id, new Flow(id, label, profile, file, coordinator, List.copyOf(workers), List.copyOf(x.machineFlows),
                    List.copyOf(x.machines), List.copyOf(x.eventCatalogs), Set.copyOf(x.smTypes), List.copyOf(taskQueues),
                    List.copyOf(prefixes)));
        }
        return out;
    }

    /** {@code showcases/<name>/ecosystem.json} by name. */
    private static Map<String, JsonNode> ecosystems(Path repo) {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        Path dir = repo.resolve("showcases");
        if (!Files.isDirectory(dir)) {
            return out;
        }
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.sorted().toList()) {
                Path json = p.resolve("ecosystem.json");
                if (Files.isRegularFile(json)) {
                    JsonNode e = Json.MAPPER.readTree(json.toFile());
                    out.put(e.path("name").asText(p.getFileName().toString()), e);
                }
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }

    /** Every flow of the class path (catalogs) and repo (ecosystem.json). */
    static FlowRegistry load(Path repo) {
        return new FlowRegistry(repo, MachineCatalog.load(), EventCatalogs.all());
    }

    private static final class Builder {
        final String id;
        final Set<String> machineFlows = new LinkedHashSet<>();
        final Set<String> machines = new LinkedHashSet<>();
        final Set<String> eventCatalogs = new LinkedHashSet<>();
        final Set<String> smTypes = new LinkedHashSet<>();
        final Set<String> taskQueues = new LinkedHashSet<>();
        final Set<String> prefixes = new LinkedHashSet<>();

        Builder(String id) {
            this.id = id;
        }
    }
}
