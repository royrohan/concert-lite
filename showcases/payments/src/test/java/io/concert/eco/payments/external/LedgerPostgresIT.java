package io.concert.eco.payments.external;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.Env;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Postgres ledger store across a restart (a new store instance on the same tables): a key committed before is
 * replayed after, never committed again. Needs Postgres ({@code LEDGER_JDBC_URL}, default the local stack's
 * {@code jdbc:postgresql://localhost:5433/concert}); {@code ./gradlew :payments:integrationTest}.
 */
@Tag("integration")
class LedgerPostgresIT {
    private static final String URL = Env.get("LEDGER_JDBC_URL", "jdbc:postgresql://localhost:5433/concert");

    private static LedgerStore store() {
        return new LedgerStore.Postgres(URL, Env.get("LEDGER_DB_USER", "concert"), Env.get("LEDGER_DB_PASSWORD", "concert"));
    }

    @Test
    void aKeyCommittedBeforeARestartIsReplayedAfterIt() {
        String id = "IT-" + System.nanoTime();
        LedgerStore.Answer before = store().charge(id + ":charge", id, new BigDecimal("42.00"), true);
        LedgerStore restarted = store();
        LedgerStore.Answer after = restarted.charge(id + ":charge", id, new BigDecimal("42.00"), true);

        assertEquals(201, before.code());
        assertFalse(before.replayed());
        assertTrue(after.replayed());
        assertEquals(before.body(), after.body());
        Map<String, Object> row = restarted.list(id).getFirst();
        assertEquals(1L, row.get("chargeCommits"));
        assertEquals(1L, row.get("replays"));
    }

    @Test
    void aRefundIsStoredAndReplayedAcrossARestart() {
        String id = "IT-" + System.nanoTime();
        store().charge(id + ":charge", id, BigDecimal.TEN, true);
        LedgerStore.Answer first = store().refund(id + ":refund", id, true);
        LedgerStore.Answer again = store().refund(id + ":refund", id, true);

        assertTrue(first.body().contains("REFUNDED"));
        assertTrue(again.replayed());
        assertEquals(1L, store().list(id).getFirst().get("refunds"));
    }
}
