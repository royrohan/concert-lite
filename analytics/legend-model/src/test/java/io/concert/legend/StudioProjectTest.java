package io.concert.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.legend.LegendStoreGenerator.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** The models and store layer as a Legend Studio project: one element per file, no imports, names in full. */
class StudioProjectTest {

    static Map<String, String> sampleSources() throws Exception {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("concert/concert.pure", Files.readString(LegendLiteCompileTest.CONCERT_PROFILE));
        try (Stream<Path> s = Files.list(LegendStoreGeneratorTest.REPO.resolve("showcases/sample-workers/src/main/pure"))) {
            for (Path p : s.filter(x -> x.toString().endsWith(".pure")).sorted().toList()) {
                out.put("models/" + p.getFileName(), Files.readString(p));
            }
        }
        return out;
    }

    static List<StudioProject.File> sampleProject() throws Exception {
        Map<String, String> sources = sampleSources();
        Map<String, String> models = new LinkedHashMap<>(sources);
        models.remove("concert/concert.pure");
        LegendStoreMain.Generated g = LegendStoreMain.fromSources(models, SchemaMapperRoots.SAMPLES);
        return StudioProject.files(sources, g, List.of(Target.DUCKDB, Target.CLICKHOUSE));
    }

    /** The sample roots (as docker-compose.yml's default SINK_ROOTS starts). */
    static final class SchemaMapperRoots {
        static final String SAMPLES = "order:demo::order::Order,payment:demo::payment::Payment,shipment:demo::shipment::Shipment";
    }

    @Test
    void oneElementPerFileWithNamesInFullAndNoImports() throws Exception {
        List<StudioProject.File> files = sampleProject();
        assertTrue(files.stream().noneMatch(f -> f.code().contains("import ")), "Studio refuses imports");
        assertEquals("concert::sm", files.getFirst().element(), "the profile first");
        StudioProject.File order = files.stream().filter(f -> f.element().equals("demo::order::Order")).findFirst().orElseThrow();
        assertTrue(order.code().contains("status: demo::order::OrderStatus[1] = demo::order::OrderStatus.CREATED;"), order.code());
        assertTrue(order.code().contains("total: demo::common::Money[0..1];"), "imported package's type in full");
        assertTrue(order.code().contains("customer: demo::order::Customer[0..1];"), "own package's type in full");
        assertFalse(order.code().contains("Class demo::order::OrderLine"), "one element per file");
        assertTrue(order.code().contains("// Documentation only"), "comments kept, not rewritten");
        StudioProject.File assoc = files.stream().filter(f -> f.element().equals("demo::order::OrderLines")).findFirst().orElseThrow();
        assertTrue(assoc.code().contains("order: demo::order::Order[1];"), assoc.code());
        assertTrue(assoc.code().startsWith("// The order owns its lines"), "the comment above an element goes with it");
        List<String> store = files.stream().map(StudioProject.File::element).filter(e -> e.startsWith("concert::")).toList();
        assertTrue(store.containsAll(List.of("concert::store::Sink", "concert::mapping::Sink", "concert::connection::DuckDb",
                "concert::connection::ClickHouse", "concert::runtime::DuckDb", "concert::runtime::ClickHouse")), store.toString());
        assertEquals(files.size(), files.stream().map(StudioProject.File::name).distinct().count());
        assertEquals(8, StudioProject.fingerprint(files).length());
    }
}
