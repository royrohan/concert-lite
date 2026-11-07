package io.concert.marketdata;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * The simulator's control endpoint (the dashboard's "simulate market data" buttons):
 *
 * <pre>
 * GET  /status          {"running":true,"tickRate":200.0,"ticks":12345,"bars":67,"ticksPerSec":199.8}
 * POST /start?rate=N    start (or change the rate of) the ticks
 * POST /stop            pause them; reference data and topics stay
 * </pre>
 */
final class ControlServer {
    private ControlServer() {}

    static HttpServer start(int port, MarketDataSim sim, MarketDataSim.Control control) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        long[] last = {System.currentTimeMillis(), 0};
        double[] perSec = {0};
        server.createContext("/status", ex -> respond(ex, status(sim, control, last, perSec)));
        server.createContext("/start", ex -> {
            if (!post(ex)) {
                return;
            }
            control.start(rate(ex));
            respond(ex, status(sim, control, last, perSec));
        });
        server.createContext("/stop", ex -> {
            if (!post(ex)) {
                return;
            }
            control.stop();
            respond(ex, status(sim, control, last, perSec));
        });
        server.start();
        System.out.printf("[marketdata-sim] control on :%d (GET /status, POST /start?rate=N, POST /stop)%n", port);
        return server;
    }

    private static synchronized String status(MarketDataSim sim, MarketDataSim.Control control, long[] last, double[] perSec) {
        long now = System.currentTimeMillis();
        long ticks = sim.ticksSent();
        if (now - last[0] >= 1000) {
            perSec[0] = (ticks - last[1]) * 1000.0 / (now - last[0]);
            last[0] = now;
            last[1] = ticks;
        }
        return String.format(Locale.ROOT, "{\"running\":%s,\"tickRate\":%.1f,\"ticks\":%d,\"bars\":%d,\"ticksPerSec\":%.1f,\"instruments\":%d}",
                control.running(), control.tickRate(), ticks, sim.barsSent(), control.running() ? perSec[0] : 0.0,
                sim.instruments().size());
    }

    private static Double rate(HttpExchange ex) {
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) {
            return null;
        }
        for (String kv : q.split("&")) {
            if (kv.startsWith("rate=")) {
                try {
                    return Double.parseDouble(kv.substring(5));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private static boolean post(HttpExchange ex) throws IOException {
        if (ex.getRequestMethod().equals("POST")) {
            return true;
        }
        byte[] b = "{\"error\":\"POST to change the simulator\"}".getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(405, b.length);
        ex.getResponseBody().write(b);
        ex.close();
        return false;
    }

    private static void respond(HttpExchange ex, String json) throws IOException {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }
}
