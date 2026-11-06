package io.concert.legend;

import io.concert.model.pure.ResolvedModel;
import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Prints (or writes) the Legend store layer for a set of models and sink roots:
 *
 * <pre>
 * LegendStoreMain [--models dir,dir] [--roots smType:pkg::Class,...] [--targets duckdb,clickhouse] [--out file]
 * </pre>
 *
 * Defaults are the sinks' own settings: {@code MODELS_DIR} and {@code SINK_ROOTS} (plus every {@code MODELS_DIR_*} /
 * {@code SINK_ROOTS_*} a compose override adds), so the store layer matches the tables the sinks build.
 */
public final class LegendStoreMain {
    private LegendStoreMain() {}

    public static void main(String[] args) throws IOException {
        Map<String, String> o = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        Map<String, String> env = System.getenv();
        String models = o.getOrDefault("models", SchemaMapper.envList(env, "MODELS_DIR", "showcases/sample-workers/src/main/pure"));
        String roots = o.getOrDefault("roots", SchemaMapper.envList(env, "SINK_ROOTS", SchemaMapper.DEFAULT_ROOTS));
        List<LegendStoreGenerator.Target> targets = Arrays.stream(o.getOrDefault("targets", "duckdb,clickhouse").split(","))
                .map(LegendStoreGenerator.Target::parse).toList();
        String pure = generate(models, roots, targets);
        if (o.containsKey("out")) {
            Files.writeString(Path.of(o.get("out")), pure, StandardCharsets.UTF_8);
        } else {
            System.out.print(pure);
        }
    }

    /**
     * The model and schema for Pure sources held in memory (file name to text; e.g. what the DuckDB sink's
     * {@code GET /models} serves) and {@code roots} (SINK_ROOTS form).
     */
    public static Generated fromSources(Map<String, String> files, String roots) {
        List<io.concert.model.pure.PureModel> models = new java.util.ArrayList<>();
        files.forEach((name, text) -> models.add(io.concert.model.pure.PureParser.parse(text, name)));
        ResolvedModel model = new io.concert.model.pure.ModelResolver().resolve(models.toArray(io.concert.model.pure.PureModel[]::new));
        SchemaSpec schema = new SchemaMapper(model).map(SchemaMapper.parseRoots(roots));
        return new Generated(model, schema, new LegendStoreGenerator(model, schema));
    }

    /** A resolved model, its sink schema and the generator over both. */
    public record Generated(ResolvedModel model, SchemaSpec schema, LegendStoreGenerator generator) {}

    /** The store layer for {@code models} (MODELS_DIR form) and {@code roots} (SINK_ROOTS form). */
    public static String generate(String models, String roots, List<LegendStoreGenerator.Target> targets) {
        ResolvedModel model = SchemaMapper.loadModels(models);
        SchemaSpec schema = new SchemaMapper(model).map(SchemaMapper.parseRoots(roots));
        return new LegendStoreGenerator(model, schema).generate(targets);
    }
}
