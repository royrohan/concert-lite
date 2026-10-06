package io.concert.sink.clickhouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Deephaven column specs for the market-data topics, generated from the same trading Pure models as
 * {@link MarketDataSchema}. The Deephaven app-mode script ({@code infra/deephaven/app.d/concert.py})
 * reads the checked-in {@code marketdata_columns.json} and builds its Kafka {@code json_spec}s from
 * it; {@code DeephavenColumnsTest} fails when the file no longer matches the models (regenerate with
 * {@code -Dconcert.regenerate=true}).
 *
 * <p>Format: {@code {"<topic>": {"class": ..., "columns": [[Column, jsonField, type], ...]}}} with
 * columns in PascalCase and types {@code string | long | double | bool | instant} ({@code instant}
 * arrives as an ISO-8601 string; the script parses it). Enums are strings.
 *
 * <p>The {@code "entity-snapshots"} entry describes the trading aggregates as they arrive in the
 * completed-entity topic: {@code {"roots": {"<smType>": {"class": ..., "columns": [[Column,
 * "/model/<field>", type], ...]}}}}, the JSON pointer reading the field out of the snapshot's
 * {@code model}. Only to-one primitive and enum properties (the association lists stay empty in these
 * aggregates, see {@code showcases/trading/README.md}).
 */
public final class DeephavenColumns {

    /** The trading roots (smType to Pure class), as in compose's {@code SINK_ROOTS}. */
    public static final Map<String, String> TRADING_ROOTS = tradingRoots();

    private static Map<String, String> tradingRoots() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("trading_order", "trading::order::Order");
        m.put("trading_execution", "trading::order::Execution");
        m.put("trading_fill", "trading::order::Fill");
        m.put("trading_allocation", "trading::order::Allocation");
        return m;
    }

    private DeephavenColumns() {}

    public static String json(ResolvedModel model) {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        ObjectNode root = mapper.createObjectNode();
        for (MarketDataSchema.Source s : MarketDataSchema.SOURCES) {
            ClassDef cls = model.findClass(s.pureClass()).orElseThrow();
            ObjectNode topic = root.putObject(s.topic());
            topic.put("class", s.pureClass());
            ArrayNode cols = topic.putArray("columns");
            for (PropertyDef p : model.allProperties(cls)) {
                if (p.multiplicity().isToOne()) {
                    cols.addArray().add(pascal(p.name())).add(p.name()).add(type(model, p));
                }
            }
        }
        ObjectNode roots = root.putObject("entity-snapshots").putObject("roots");
        for (Map.Entry<String, String> r : TRADING_ROOTS.entrySet()) {
            ClassDef cls = model.findClass(r.getValue()).orElseThrow();
            ObjectNode t = roots.putObject(r.getKey());
            t.put("class", r.getValue());
            ArrayNode cols = t.putArray("columns");
            for (PropertyDef p : model.allProperties(cls)) {
                if (p.multiplicity().isToOne()) {
                    cols.addArray().add(pascal(p.name())).add("/model/" + p.name()).add(type(model, p));
                }
            }
        }
        try {
            return mapper.writeValueAsString(root) + "\n";
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    static String type(ResolvedModel model, PropertyDef p) {
        if (model.isEnum(p.type()) || !p.type().primitive()) {
            return "string";
        }
        return switch (p.type().primitiveType()) {
            case STRING, DATE, STRICT_DATE -> "string";
            case INTEGER -> "long";
            case FLOAT, NUMBER, DECIMAL -> "double";
            case BOOLEAN -> "bool";
            case DATE_TIME -> "instant";
        };
    }

    static String pascal(String camel) {
        return camel.isEmpty() ? camel : camel.substring(0, 1).toUpperCase(Locale.ROOT) + camel.substring(1);
    }
}
