package io.concert.common;

/** Tiny env-var helper; every component is configured through environment variables. */
public final class Env {
    private Env() {}

    public static String get(String name, String dflt) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? System.getProperty(name, dflt) : v;
    }

    public static int getInt(String name, int dflt) {
        return Integer.parseInt(get(name, Integer.toString(dflt)));
    }

    public static long getLong(String name, long dflt) {
        return Long.parseLong(get(name, Long.toString(dflt)));
    }

    public static boolean getBool(String name, boolean dflt) {
        return Boolean.parseBoolean(get(name, Boolean.toString(dflt)));
    }
}
