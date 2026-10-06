package io.concert.trading.machines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.TaskQueues;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.TransitionResult;
import io.concert.marketdata.PriceModel;
import io.concert.model.runtime.ModelJson;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sink.EntitySnapshot;
import io.concert.store.InMemoryStateStore;
import io.concert.trading.TradingEvents;
import io.concert.trading.TradingFlowGenerator;
import io.concert.trading.TradingFlows;
import io.concert.trading.command.AckCommand;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.CloseOrderCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.NewOrderCommand;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.command.RouteCommand;
import io.concert.trading.common.Liquidity;
import io.concert.trading.common.OrderType;
import io.concert.trading.common.Side;
import io.concert.trading.order.Allocation;
import io.concert.trading.order.Execution;
import io.concert.trading.order.Fill;
import io.concert.trading.order.FillStatus;
import io.concert.trading.order.Order;
import io.concert.trading.order.OrderStatus;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The four trading machines on a Temporal test server with an in-memory store. The orchestrator is
 * simulated: each event goes to its entity workflow with UpdateWithStart, one at a time, in the order
 * the generator emits them (which keeps every entity's events in order, as the lock manager would).
 */
class TradingMachinesTest {

    private static final long START = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();

    private TestWorkflowEnvironment env;
    private InMemoryStateStore store;
    private WorkflowClient client;
    private final List<EntitySnapshot> snapshots = new CopyOnWriteArrayList<>();
    private final AtomicLong eventIds = new AtomicLong();

    @BeforeEach
    void setUp() {
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(false).build());
        store = new InMemoryStateStore();
        TradingMachines.ALL.forEach((type, m) -> {
            Worker w = env.newWorker(TaskQueues.stateMachine(type));
            w.registerWorkflowImplementationTypes(m.impl());
            w.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store),
                    (EntitySnapshotPublisher) snapshots::add);
        });
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private TransitionResult send(EventEnvelope e) {
        EntityWorkflow wf = client.newWorkflowStub(EntityWorkflow.class, WorkflowOptions.newBuilder()
                .setWorkflowId(WorkflowIds.entity(e.smType(), e.instanceKey()))
                .setTaskQueue(TaskQueues.stateMachine(e.smType()))
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build());
        return WorkflowClient.executeUpdateWithStart(wf::handle, e,
                UpdateOptions.<TransitionResult>newBuilder().setUpdateId(e.eventId())
                        .setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                new WithStartWorkflowOperation<>(wf::run, EntityInit.fresh(e.smType(), e.instanceKey())));
    }

    private TransitionResult send(String smType, String eventType, OrderEvent payload) {
        return send(TradingEvents.envelope(smType, eventType, "x-e" + eventIds.incrementAndGet(), payload,
                payload.getTs().toEpochMilli()));
    }

    private <T> T data(String smType, String key, Class<T> type) {
        return ModelJson.read(store.loadState(WorkflowIds.entityKey(smType, key)).orElseThrow().data(), type);
    }

    private String state(String entityKey) {
        return store.loadState(entityKey).orElseThrow().state();
    }

    private static void await(String what, long timeoutMs, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            LockSupport.parkNanos(20_000_000);
        }
    }

    // ---------------------------------------------------------------- generated flow end to end

    @Test
    void generatedFlowRunsThroughTheMachines() {
        TradingFlowGenerator.Config config = new TradingFlowGenerator.Config(11, PriceModel.DEFAULT_VOL_MULTIPLIER, "m", 24,
                "all", "all", 6, START, 200, 0.05, 0.15, 0.5);
        List<EventEnvelope> events = new ArrayList<>();
        new TradingFlowGenerator(config).forEachRemaining(events::add);

        Map<String, List<FillCommand>> fillsByOrder = new HashMap<>();
        Map<String, String> entities = new LinkedHashMap<>(); // entityKey -> smType
        for (EventEnvelope e : events) {
            TransitionResult r = send(e);
            assertTrue(r.accepted(), () -> e.entityKey() + " " + e.eventType() + ": " + r.message());
            entities.put(e.entityKey(), e.smType());
            if (e.smType().equals(TradingFlows.FILL)) {
                FillCommand f = ModelJson.read(e.payload(), FillCommand.class);
                fillsByOrder.computeIfAbsent(f.getOrderId(), k -> new ArrayList<>()).add(f);
            }
        }

        // Allocations by order, from the allocation entities' own data.
        Map<String, Long> allocatedByOrder = new HashMap<>();
        int filledOrders = 0;
        int cancelledWithFills = 0;
        for (Map.Entry<String, String> en : entities.entrySet()) {
            String key = en.getKey().substring(en.getValue().length() + 1);
            switch (en.getValue()) {
                case TradingFlows.FILL -> {
                    Fill f = data(TradingFlows.FILL, key, Fill.class);
                    assertEquals(FillStatus.BOOKED, f.getStatus());
                    assertEquals("BOOKED", state(en.getKey()));
                }
                case TradingFlows.ALLOCATION -> {
                    Allocation a = data(TradingFlows.ALLOCATION, key, Allocation.class);
                    assertEquals("CONFIRMED", state(en.getKey()));
                    assertNotNull(a.getConfirmedAt());
                    allocatedByOrder.merge(a.getOrderId(), a.getQuantity(), Long::sum);
                }
                case TradingFlows.EXECUTION -> {
                    Execution x = data(TradingFlows.EXECUTION, key, Execution.class);
                    String s = state(en.getKey());
                    assertTrue(s.equals("FILLED") || s.equals("CANCELLED"), s);
                    if (s.equals("FILLED")) {
                        assertEquals(x.getQuantity(), x.getFilledQty());
                    }
                    if (x.getFilledQty() > 0) {
                        List<FillCommand> fs = fillsByOrder.get(x.getOrderId()).stream()
                                .filter(f -> f.getExecId().equals(key)).toList();
                        assertTrue(x.getAvgPx().subtract(vwap(fs)).abs().compareTo(new BigDecimal("0.000001")) <= 0,
                                () -> key + " avgPx " + x.getAvgPx() + " vs " + vwap(fs));
                    }
                }
                default -> { }
            }
        }
        for (Map.Entry<String, String> en : entities.entrySet()) {
            if (!en.getValue().equals(TradingFlows.ORDER)) {
                continue;
            }
            String orderId = en.getKey().substring(TradingFlows.ORDER.length() + 1);
            Order o = data(TradingFlows.ORDER, orderId, Order.class);
            assertEquals(o.getStatus().name(), state(en.getKey()));
            assertTrue(o.getExecutions().isEmpty() && o.getAllocations().isEmpty(), "order keeps aggregates only");
            List<FillCommand> fills = fillsByOrder.getOrDefault(orderId, List.of());
            long filled = fills.stream().mapToLong(FillCommand::getQuantity).sum();
            assertEquals(filled, o.getFilledQty());
            switch (o.getStatus()) {
                case FILLED -> {
                    filledOrders++;
                    assertEquals(o.getQuantity(), o.getFilledQty());
                    assertEquals(0L, o.getLeavesQty());
                    assertEquals(0, vwap(fills).compareTo(o.getAvgPx()), () -> orderId + " avgPx " + o.getAvgPx());
                    assertEquals(o.getFilledQty(), allocatedByOrder.get(orderId));
                    assertEquals(fills.stream().map(FillCommand::getFee).reduce(BigDecimal.ZERO, BigDecimal::add)
                            .compareTo(o.getTotalFees()), 0);
                    assertEquals(fills.getFirst().getTs(), o.getFirstFillAt());
                    assertEquals(fills.getLast().getTs(), o.getLastFillAt());
                    assertNotNull(o.getCompletedAt());
                }
                case CANCELLED -> {
                    if (filled > 0) {
                        cancelledWithFills++;
                        assertEquals(0, vwap(fills).compareTo(o.getAvgPx()));
                        assertEquals(filled, allocatedByOrder.get(orderId));
                    }
                    assertNotNull(o.getCompletedAt());
                }
                case REJECTED -> assertEquals(0L, o.getFilledQty());
                default -> throw new AssertionError(orderId + " not terminal: " + o.getStatus());
            }
        }
        assertTrue(filledOrders > 10, "filled orders: " + filledOrders);

        // Exactly one snapshot per entity (all end terminal), matching the stored final state and data.
        await("snapshots", 20_000, () -> snapshots.size() >= entities.size());
        LockSupport.parkNanos(300_000_000);
        assertEquals(entities.size(), snapshots.size());
        Map<String, EntitySnapshot> byId = new HashMap<>();
        for (EntitySnapshot s : snapshots) {
            assertTrue(byId.put(s.entityId(), s) == null, "duplicate snapshot " + s.entityId());
            assertEquals(entities.get(s.entityId()), s.smType());
            var stored = store.loadState(s.entityId()).orElseThrow();
            assertEquals(stored.state(), s.state());
            assertEquals(stored.data(), s.modelJson());
            assertTrue(TradingFlows.flow(s.smType()).terminalStates().contains(s.state()), s.state());
        }
        assertEquals(entities.keySet(), byId.keySet());
        System.out.printf("trading flow: %d events, %d entities, %d filled orders, %d cancelled with fills%n",
                events.size(), entities.size(), filledOrders, cancelledWithFills);
    }

    /** Quantity-weighted average price at 6 decimals, half-even (the order machine's definition). */
    private static BigDecimal vwap(List<FillCommand> fills) {
        BigDecimal notional = BigDecimal.ZERO;
        long qty = 0;
        for (FillCommand f : fills) {
            notional = notional.add(f.getPrice().multiply(BigDecimal.valueOf(f.getQuantity())));
            qty += f.getQuantity();
        }
        return notional.divide(BigDecimal.valueOf(qty), 6, RoundingMode.HALF_EVEN);
    }

    // ---------------------------------------------------------------- business rules

    private static final String ACCT = "ACC-001";
    private static final Instant T0 = Instant.parse("2026-10-05T15:00:00Z");

    private static Instant t(int seconds) {
        return T0.plusSeconds(seconds);
    }

    /** submit + ack + one route of the full quantity: the order is WORKING. */
    private void working(String orderId, Side side, OrderType type, String limit, long qty) {
        assertTrue(send(TradingFlows.ORDER, "submit", new NewOrderCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(0))
                .setClientId("CL-001").setSymbol("AAPL").setSide(side).setOrderType(type)
                .setLimitPrice(limit == null ? null : new BigDecimal(limit)).setQuantity(qty)
                .setArrivalPx(new BigDecimal("100.00"))).accepted());
        assertTrue(send(TradingFlows.ORDER, "ack", new AckCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(1))).accepted());
        assertTrue(send(TradingFlows.ORDER, "route", new RouteCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(2))
                .setExecId(orderId + "-E1").setVenue("XNAS").setSymbol("AAPL").setSide(side).setQuantity(qty)
                .setLimitPrice(limit == null ? null : new BigDecimal(limit))).accepted());
    }

    private static FillCommand fill(String orderId, int n, Side side, long qty, String px) {
        return new FillCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(10 + n)).setFillId(orderId + "-F" + n)
                .setExecId(orderId + "-E1").setSymbol("AAPL").setSide(side).setQuantity(qty).setPrice(new BigDecimal(px))
                .setVenue("XNAS").setLiquidity(Liquidity.REMOVE).setFee(new BigDecimal("0.30"));
    }

    private static AllocateCommand alloc(String orderId, int n, long qty, String avgPx) {
        return new AllocateCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(30 + n)).setAllocId(orderId + "-A" + n)
                .setAllocAccountId("ACC-00" + (n + 1)).setSymbol("AAPL").setSide(Side.BUY).setQuantity(qty)
                .setAvgPx(new BigDecimal(avgPx));
    }

    private static CloseOrderCommand close(String orderId, long allocated, long count) {
        return new CloseOrderCommand().setOrderId(orderId).setAccountId(ACCT).setTs(t(60)).setAllocatedQty(allocated)
                .setAllocationCount(count);
    }

    @Test
    void overfillIsRejected() {
        working("O1", Side.BUY, OrderType.MARKET, null, 100);
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O1", 1, Side.BUY, 60, "100.10")).accepted());
        TransitionResult r = send(TradingFlows.ORDER, "complete_fill", fill("O1", 2, Side.BUY, 60, "100.20"));
        assertFalse(r.accepted());
        assertTrue(r.message().contains("overfill"), r.message());
        assertFalse(send(TradingFlows.ORDER, "fill", fill("O1", 2, Side.BUY, 60, "100.20")).accepted());
        // a fill that would complete the order must be complete_fill
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O1", 2, Side.BUY, 40, "100.20")).message().contains("complete_fill"));

        Order o = data(TradingFlows.ORDER, "O1", Order.class);
        assertEquals(OrderStatus.PARTIALLY_FILLED, o.getStatus());
        assertEquals(60L, o.getFilledQty());
        assertEquals(40L, o.getLeavesQty());
        assertEquals(0, new BigDecimal("100.10").compareTo(o.getAvgPx()));

        TransitionResult done = send(TradingFlows.ORDER, "complete_fill", fill("O1", 2, Side.BUY, 40, "100.20"));
        assertTrue(done.accepted(), done.message());
        o = data(TradingFlows.ORDER, "O1", Order.class);
        assertEquals(OrderStatus.ALLOCATING, o.getStatus());
        assertEquals(0, new BigDecimal("100.14").compareTo(o.getAvgPx()), o.getAvgPx().toPlainString()); // (6006+4008)/100
        assertEquals(0, new BigDecimal("0.60").compareTo(o.getTotalFees()));
        assertEquals(t(11), o.getFirstFillAt());
        assertEquals(t(12), o.getLastFillAt());

        // execution machine: overfill of the child order
        EventEnvelope route = TradingEvents.envelope(TradingFlows.EXECUTION, "route", "x-r", new RouteCommand().setOrderId("O1")
                .setAccountId(ACCT).setTs(t(2)).setExecId("O1-E1").setVenue("XNAS").setSymbol("AAPL").setSide(Side.BUY)
                .setQuantity(100L), t(2).toEpochMilli());
        assertTrue(send(route).accepted());
        assertFalse(send(TradingFlows.EXECUTION, "complete_fill", fill("O1", 1, Side.BUY, 101, "100.10")).accepted());
    }

    @Test
    void limitViolationIsRejected() {
        working("O2", Side.BUY, OrderType.LIMIT, "100.00", 100);
        TransitionResult r = send(TradingFlows.ORDER, "fill", fill("O2", 1, Side.BUY, 50, "100.01"));
        assertFalse(r.accepted());
        assertTrue(r.message().contains("limit violation"), r.message());
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O2", 1, Side.BUY, 50, "100.00")).accepted()); // at the limit

        working("O3", Side.SELL, OrderType.LIMIT, "100.00", 100);
        assertFalse(send(TradingFlows.ORDER, "fill", fill("O3", 1, Side.SELL, 50, "99.99")).accepted());
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O3", 1, Side.SELL, 50, "100.05")).accepted());

        working("O4", Side.SELL_SHORT, OrderType.LIMIT, "100.00", 100);
        assertFalse(send(TradingFlows.ORDER, "fill", fill("O4", 1, Side.SELL_SHORT, 50, "99.50")).accepted());

        // MARKET orders have no price check
        working("O5", Side.BUY, OrderType.MARKET, null, 100);
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O5", 1, Side.BUY, 50, "250.00")).accepted());
        assertEquals(0L, data(TradingFlows.ORDER, "O4", Order.class).getFilledQty());
    }

    @Test
    void closeBeforeFullAllocationIsRejected() {
        working("O6", Side.BUY, OrderType.MARKET, null, 100);
        assertTrue(send(TradingFlows.ORDER, "complete_fill", fill("O6", 1, Side.BUY, 100, "100.10")).accepted());
        assertTrue(send(TradingFlows.ORDER, "allocate", alloc("O6", 1, 70, "100.10")).accepted());

        TransitionResult early = send(TradingFlows.ORDER, "close", close("O6", 70, 1));
        assertFalse(early.accepted());
        assertTrue(early.message().contains("cannot close"), early.message());

        TransitionResult over = send(TradingFlows.ORDER, "allocate", alloc("O6", 2, 40, "100.10"));
        assertFalse(over.accepted());
        assertTrue(over.message().contains("over-allocation"), over.message());

        assertTrue(send(TradingFlows.ORDER, "allocate", alloc("O6", 2, 30, "100.10")).accepted());
        assertFalse(send(TradingFlows.ORDER, "close", close("O6", 90, 2)).accepted()); // command disagrees
        TransitionResult closed = send(TradingFlows.ORDER, "close", close("O6", 100, 2));
        assertTrue(closed.accepted(), closed.message());

        Order o = data(TradingFlows.ORDER, "O6", Order.class);
        assertEquals(OrderStatus.FILLED, o.getStatus());
        assertEquals(100L, o.getAllocatedQty());
        assertEquals(t(60), o.getCompletedAt());
        await("order snapshot", 10_000, () -> !snapshots.isEmpty());
        LockSupport.parkNanos(300_000_000);
        assertEquals(1, snapshots.size());
        EntitySnapshot s = snapshots.getFirst();
        assertEquals("trading_order:O6", s.entityId());
        assertEquals(TradingFlows.ORDER, s.smType());
        assertEquals("FILLED", s.state());
        assertEquals(store.loadState("trading_order:O6").orElseThrow().data(), s.modelJson());
    }

    @Test
    void historiesReplayDeterministically() throws Exception {
        working("O7", Side.BUY, OrderType.LIMIT, "101.00", 100);
        assertTrue(send(TradingFlows.ORDER, "fill", fill("O7", 1, Side.BUY, 40, "100.10")).accepted());
        assertFalse(send(TradingFlows.ORDER, "fill", fill("O7", 2, Side.BUY, 40, "101.50")).accepted());
        assertTrue(send(TradingFlows.ORDER, "complete_fill", fill("O7", 2, Side.BUY, 60, "100.30")).accepted());
        assertTrue(send(TradingFlows.ORDER, "allocate", alloc("O7", 1, 100, "100.22")).accepted());
        assertTrue(send(TradingFlows.ORDER, "close", close("O7", 100, 1)).accepted());
        await("order snapshot", 10_000, () -> !snapshots.isEmpty());
        WorkflowReplayer.replayWorkflowExecution(client.fetchHistory("trading_order:O7"), TradingOrderMachine.class);
    }
}
