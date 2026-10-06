package io.concert.marketdata;

import java.nio.charset.StandardCharsets;

/**
 * Counter-based randomness: every random number is a pure function of its coordinates (seed, symbol,
 * purpose, index). That is what lets two processes (the market data simulator and the trading
 * generator) compute the same quote for the same symbol and instant without talking to each other.
 */
final class Hashing {
    private Hashing() {}

    /** SplitMix64 finalizer: a well-mixed 64-bit permutation. */
    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    static long hash(long a, long b, long c, long d) {
        long h = mix(a + 0x9e3779b97f4a7c15L);
        h = mix(h ^ b);
        h = mix(h ^ c);
        return mix(h ^ d);
    }

    /** Stable 64-bit FNV-1a of a string (unlike {@code String.hashCode}, well spread in 64 bits). */
    static long fnv(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        return h;
    }

    /** Uniform in the open interval (0, 1). */
    static double uniform(long h) {
        return ((h >>> 11) + 0.5) * 0x1.0p-53;
    }

    /** Standard normal via Box-Muller over two derived uniforms. */
    static double gaussian(long h) {
        double u1 = uniform(h);
        double u2 = uniform(mix(h ^ 0x632be59bd9b4e019L));
        return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
    }
}
