package io.concert.sdk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of state machines a module provides, discovered with {@link ServiceLoader} (declare the
 * implementation in {@code META-INF/services/io.concert.sdk.MachineCatalog}). Tools that show every
 * machine (the trace UI) load all catalogs on their classpath instead of naming the modules: the
 * sample workers, trading and every generated ecosystem ({@code ./generate-concert-ecosystem})
 * implement it.
 */
public interface MachineCatalog {

    /**
     * One machine.
     *
     * @param modelMermaid Mermaid class diagram of the machine's Pure model, or {@code null} if untyped
     */
    record Machine(String smType, Class<? extends AbstractStateMachine> impl, StateMachineSpec spec, String modelMermaid) {}

    /** Name of the catalog, e.g. {@code samples}, {@code trading} or the ecosystem name. */
    String name();

    /** The machines, in display order. */
    List<Machine> machines();

    /** Every catalog on the class path, sorted by name. */
    static List<MachineCatalog> load() {
        List<MachineCatalog> catalogs = new ArrayList<>();
        ServiceLoader.load(MachineCatalog.class).forEach(catalogs::add);
        catalogs.sort(Comparator.comparing(MachineCatalog::name));
        return catalogs;
    }

    /**
     * Every machine of every catalog by smType. If two catalogs define the same smType the first (by
     * catalog name) wins and the other is logged and skipped.
     */
    static Map<String, Machine> all() {
        Logger log = LoggerFactory.getLogger(MachineCatalog.class);
        Map<String, Machine> out = new LinkedHashMap<>();
        Map<String, String> owner = new LinkedHashMap<>();
        for (MachineCatalog c : load()) {
            for (Machine m : c.machines()) {
                if (out.putIfAbsent(m.smType(), m) != null) {
                    log.warn("smType {} of catalog {} is already defined by catalog {}; skipped", m.smType(), c.name(),
                            owner.get(m.smType()));
                } else {
                    owner.put(m.smType(), c.name());
                }
            }
        }
        return out;
    }

    /** Registers the specs of every catalog's machines in {@link StateMachineRegistry}; returns them by smType. */
    static Map<String, Machine> registerAll() {
        Map<String, Machine> all = all();
        all.forEach((type, m) -> StateMachineRegistry.register(type, m.spec()));
        return all;
    }
}
