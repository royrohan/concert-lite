package io.concert.tools;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** {@code tools loadgen --run-id X ...} or {@code tools verify --manifest m.json ...}. */
public final class ToolsMain {
    private ToolsMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: tools loadgen|verify [--key value ...]");
            System.exit(2);
        }
        Map<String, String> opts = parse(Arrays.copyOfRange(args, 1, args.length));
        int code = switch (args[0]) {
            case "loadgen" -> LoadGen.run(opts);
            case "verify" -> Verify.run(opts);
            default -> {
                System.err.println("unknown command " + args[0]);
                yield 2;
            }
        };
        System.exit(code);
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        return m;
    }
}
