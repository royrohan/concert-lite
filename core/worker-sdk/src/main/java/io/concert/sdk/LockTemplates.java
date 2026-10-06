package io.concert.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Lock key templates, as declared with {@code concert::sm.locks} in Pure models: literal text with
 * {@code {field}} placeholders. {@code {id}} is the target instance key, any other {@code {name}} the
 * top-level field {@code name} of the event payload, e.g. {@code policy:{policyId}} renders as
 * {@code policy:POL-7}.
 */
public final class LockTemplates {
    private LockTemplates() {}

    /** Placeholder for the instance key. */
    public static final String ID = "id";

    /** The placeholder names of a template, in order. */
    public static List<String> placeholders(String template) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while ((i = template.indexOf('{', i)) >= 0) {
            int j = template.indexOf('}', i);
            if (j < 0) {
                throw new IllegalArgumentException("unclosed '{' in lock template " + template);
            }
            out.add(template.substring(i + 1, j).trim());
            i = j + 1;
        }
        return out;
    }

    /**
     * Renders a template.
     *
     * @param field payload field value by name ({@code null} if missing)
     * @throws IllegalArgumentException if a placeholder has no value
     */
    public static String render(String template, String instanceKey, Function<String, String> field) {
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
                throw new IllegalArgumentException("unclosed '{' in lock template " + template);
            }
            sb.append(template, i, open);
            String name = template.substring(open + 1, close).trim();
            String value = name.equals(ID) ? instanceKey : field.apply(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("lock template " + template + " needs payload field '" + name + "'");
            }
            sb.append(value);
            i = close + 1;
        }
        return sb.toString();
    }

    /** Renders templates against a JSON payload (top-level fields; {@code null} payload has none). */
    public static List<String> render(List<String> templates, String instanceKey, JsonNode payload) {
        Function<String, String> field = name -> {
            JsonNode n = payload == null ? null : payload.get(name);
            return n == null || n.isNull() || n.isContainerNode() ? null : n.asText();
        };
        return templates.stream().map(t -> render(t, instanceKey, field)).toList();
    }
}
