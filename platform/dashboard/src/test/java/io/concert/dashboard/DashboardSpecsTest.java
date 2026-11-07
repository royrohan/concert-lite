package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.sdk.StateMachineRegistry;
import org.junit.jupiter.api.Test;

/** The dashboard knows the sample, trading and generated machines: state diagrams, model diagrams, widgets. */
class DashboardSpecsTest {

    @Test
    void tradingMachinesAreRegisteredWithModelDiagrams() {
        DashboardMain.registerSpecs();
        for (String type : new String[] {"order", "payment", "trading_order", "trading_execution", "trading_fill", "trading_allocation"}) {
            assertNotNull(StateMachineRegistry.get(type), type);
            assertNotNull(DashboardMain.MODEL_DIAGRAMS.get(type), type);
        }
        assertTrue(StateMachineRegistry.get("trading_order").toMermaid("WORKING").contains("ALLOCATING"));
        assertTrue(DashboardMain.MODEL_DIAGRAMS.get("trading_fill").startsWith("classDiagram"));
        assertTrue(DashboardMain.DEEPHAVEN_WIDGETS.contains("account_pnl"));
        assertTrue(DashboardMain.DEEPHAVEN_NOTEBOOKS.contains("05_trading_pnl.py"));
        assertTrue(DashboardMain.DEEPHAVEN_WIDGETS.contains("basket_value_chart"));
        assertTrue(DashboardMain.DEEPHAVEN_NOTEBOOKS.contains("06_order_basket.py"));
    }

    /** Generated ecosystems are found through the MachineCatalog SPI: no dashboard edit per ecosystem. */
    @Test
    void generatedEcosystemsAreDiscovered() {
        DashboardMain.registerSpecs();
        for (String type : new String[] {"claim", "policy", "payout", "gen_trading_order", "gen_trading_fill"}) {
            assertNotNull(StateMachineRegistry.get(type), type);
            assertTrue(DashboardMain.MODEL_DIAGRAMS.get(type).startsWith("classDiagram"), type);
        }
        assertTrue(StateMachineRegistry.get("claim").toMermaid("ASSESSED").contains("APPROVED --> PAID : pay"));
    }
}
