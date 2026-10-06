package io.concert.traceui;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.sdk.StateMachineRegistry;
import org.junit.jupiter.api.Test;

/** The trace UI knows the sample, trading and generated machines: state diagrams, model diagrams, widgets. */
class TraceUiSpecsTest {

    @Test
    void tradingMachinesAreRegisteredWithModelDiagrams() {
        TraceUiMain.registerSpecs();
        for (String type : new String[] {"order", "payment", "trading_order", "trading_execution", "trading_fill", "trading_allocation"}) {
            assertNotNull(StateMachineRegistry.get(type), type);
            assertNotNull(TraceUiMain.MODEL_DIAGRAMS.get(type), type);
        }
        assertTrue(StateMachineRegistry.get("trading_order").toMermaid("WORKING").contains("ALLOCATING"));
        assertTrue(TraceUiMain.MODEL_DIAGRAMS.get("trading_fill").startsWith("classDiagram"));
        assertTrue(TraceUiMain.DEEPHAVEN_WIDGETS.contains("account_pnl"));
        assertTrue(TraceUiMain.DEEPHAVEN_NOTEBOOKS.contains("05_trading_pnl.py"));
    }

    /** Generated ecosystems are found through the MachineCatalog SPI: no trace UI edit per ecosystem. */
    @Test
    void generatedEcosystemsAreDiscovered() {
        TraceUiMain.registerSpecs();
        for (String type : new String[] {"claim", "policy", "payout", "gen_trading_order", "gen_trading_fill"}) {
            assertNotNull(StateMachineRegistry.get(type), type);
            assertTrue(TraceUiMain.MODEL_DIAGRAMS.get(type).startsWith("classDiagram"), type);
        }
        assertTrue(StateMachineRegistry.get("claim").toMermaid("ASSESSED").contains("APPROVED --> PAID : pay"));
    }
}
