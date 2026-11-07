package io.concert.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.common.Json;
import java.util.Map;

/**
 * Renders the declared emissions of a transition ({@link StateMachineSpec.Emission}, {@code concert::sm.emits}): the
 * source is the entity's data after the transition overlaid with the triggering event's payload; templates are literal
 * text with {@code {field}} placeholders over the source's top-level fields. Pure functions of their inputs, so safe in
 * workflow code.
 */
public final class ChainEmissions {
    private ChainEmissions() {}

    /** The entity's data overlaid with the triggering event's payload (either may be {@code null}). */
    public static ObjectNode source(String dataJson, String payloadJson) {
        ObjectNode out = Json.MAPPER.createObjectNode();
        for (String json : new String[] {dataJson, payloadJson}) {
            if (json != null && !json.isBlank()) {
                JsonNode n = Json.read(json, JsonNode.class);
                if (n instanceof ObjectNode o) {
                    out.setAll(o);
                }
            }
        }
        return out;
    }

    /** The emitted payload: the source overlaid with the rendered field templates. */
    public static ObjectNode payload(StateMachineSpec.Emission e, ObjectNode source) {
        ObjectNode out = source.deepCopy();
        for (Map.Entry<String, String> f : e.fields().entrySet()) {
            String t = f.getValue().strip();
            String single = singlePlaceholder(t);
            if (single != null) {
                JsonNode v = source.get(single);
                if (v == null || v.isNull()) {
                    throw new IllegalArgumentException("emit " + e.smType() + "." + e.eventType() + ": no value for {" + single
                            + "} (field " + f.getKey() + ")");
                }
                out.set(f.getKey(), v.deepCopy());
            } else {
                out.put(f.getKey(), render(t, source, e));
            }
        }
        return out;
    }

    /** Renders a template's {@code {field}} placeholders from the source. */
    public static String render(String template, ObjectNode source, StateMachineSpec.Emission e) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            int open = template.indexOf('{', i);
            if (open < 0) {
                sb.append(template, i, template.length());
                break;
            }
            int close = template.indexOf('}', open);
            if (close < 0) {
                throw new IllegalArgumentException("unclosed '{' in " + template);
            }
            sb.append(template, i, open);
            String name = template.substring(open + 1, close).strip();
            JsonNode v = source.get(name);
            if (v == null || v.isNull()) {
                throw new IllegalArgumentException("emit " + e.smType() + "." + e.eventType() + ": no value for {" + name + "} in '"
                        + template + "'");
            }
            sb.append(v.isValueNode() ? v.asText() : Json.write(v));
            i = close + 1;
        }
        return sb.toString();
    }

    /** {@code name} if the template is exactly {@code {name}}, else {@code null}. */
    private static String singlePlaceholder(String t) {
        if (t.length() > 2 && t.charAt(0) == '{' && t.charAt(t.length() - 1) == '}' && t.indexOf('{', 1) < 0) {
            return t.substring(1, t.length() - 1).strip();
        }
        return null;
    }
}
