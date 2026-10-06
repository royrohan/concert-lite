package io.concert.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.model.pure.AssociationEnd;
import io.concert.model.runtime.ModelObject;
import io.concert.trading.command.OrderEvent;
import io.concert.trading.model.TradingModels;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.EnumValueDef;
import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The Pure models parse and resolve, every type is generated, and TradingFlows agrees with them. */
class TradingModelTest {

    private static ResolvedModel model;

    @BeforeAll
    static void resolve() throws IOException {
        Path dir = Path.of(System.getProperty("trading.pureDir", "../../core/trading-model/src/main/pure"));
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.filter(p -> p.toString().endsWith(".pure")).sorted().toList();
        }
        assertEquals(Set.of(TradingModels.FILES), files.stream().map(p -> p.getFileName().toString()).collect(Collectors.toSet()),
                "TradingModels generates every model file");
        PureModel[] parsed = new PureModel[files.size()];
        for (int i = 0; i < parsed.length; i++) {
            parsed[i] = PureParser.parse(files.get(i));
        }
        model = new ModelResolver().resolve(parsed); // throws PureModelException listing every error
    }

    private static ClassDef cls(String qualifiedName) {
        return model.findClass(qualifiedName).orElseThrow(() -> new AssertionError("missing class " + qualifiedName));
    }

    private static List<String> props(String qualifiedName) {
        return model.allProperties(cls(qualifiedName)).stream().map(PropertyDef::name).toList();
    }

    private static List<String> enumValues(String qualifiedName) {
        EnumDef e = model.findEnum(qualifiedName).orElseThrow(() -> new AssertionError("missing enum " + qualifiedName));
        return e.values().stream().map(EnumValueDef::name).toList();
    }

    private static List<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toList();
    }

    @Test
    void resolvesWithoutWarnings() {
        assertEquals(List.of(), model.warnings());
        assertEquals(3 + 3 + 4 + 10, model.classes().size(), "refdata 3 + marketdata 3 + aggregates 4 + OrderEvent and 9 commands");
    }

    @Test
    void aggregatesOwnTheirChildren() {
        assertOwns("trading::order::Order", "executions", "trading::order::Execution");
        assertOwns("trading::order::Order", "allocations", "trading::order::Allocation");
        assertOwns("trading::order::Execution", "fills", "trading::order::Fill");
        AssociationEnd back = model.associationProperties(cls("trading::order::Fill")).getFirst();
        assertEquals("execution", back.navigable().name());
        assertTrue(!back.owned() && back.navigable().multiplicity().isOptional(), "back references stay optional");
    }

    private static void assertOwns(String owner, String property, String child) {
        AssociationEnd end = model.associationProperties(cls(owner)).stream()
                .filter(e -> e.navigable().name().equals(property)).findFirst()
                .orElseThrow(() -> new AssertionError(owner + " has no association end " + property));
        assertTrue(end.owned(), owner + "." + property + " is owned");
        assertTrue(end.navigable().multiplicity().isToMany());
        assertEquals(child, end.navigable().type().name());
    }

    @Test
    void keyClassesHaveTheirFields() {
        assertTrue(props("trading::order::Order").containsAll(List.of("orderId", "clientId", "accountId", "symbol", "side",
                "orderType", "limitPrice", "quantity", "filledQty", "avgPx", "status", "timeInForce", "createdAt")));
        assertTrue(props("trading::order::Fill").containsAll(List.of("fillId", "execId", "orderId", "quantity", "price",
                "venue", "liquidity", "fee", "ts")));
        assertEquals("Decimal", typeOf("trading::order::Fill", "price"));
        assertEquals("Decimal", typeOf("trading::order::Order", "limitPrice"));
        assertEquals("Decimal", typeOf("trading::refdata::Instrument", "tickSize"));
        assertTrue(model.allProperties(cls("trading::order::Order")).stream()
                .filter(p -> p.name().equals("limitPrice")).findFirst().orElseThrow().multiplicity().isOptional());
    }

    private static String typeOf(String cls, String prop) {
        return model.allProperties(cls(cls)).stream().filter(p -> p.name().equals(prop)).findFirst().orElseThrow().type().name();
    }

    @Test
    void flowStatesAreTheStatusEnums() {
        for (TradingFlows.Flow f : TradingFlows.all()) {
            assertEquals(Set.copyOf(enumValues(TradingFlows.ORDER_PACKAGE + f.statusEnum())), f.states(), f.smType());
            String root = TradingFlows.ORDER_PACKAGE + f.modelClass();
            assertTrue(props(root).contains(f.keyField()), root + " has key " + f.keyField());
            assertTrue(props(root).contains("status"));
        }
    }

    @Test
    void payloadClassesExistAndCarryKeyAndLockFields() {
        for (TradingFlows.Flow f : TradingFlows.all()) {
            for (TradingFlows.Transition t : f.transitions()) {
                ClassDef payload = cls(t.qualifiedPayloadClass());
                List<String> fields = props(payload.qualifiedName());
                assertTrue(fields.contains(f.keyField()), t + " payload has " + f.keyField());
                for (TradingFlows.LockKey k : f.locksFor(t.eventType())) {
                    PropertyDef p = model.allProperties(payload).stream().filter(x -> x.name().equals(k.payloadField()))
                            .findFirst().orElseThrow(() -> new AssertionError(t + " payload lacks lock field " + k));
                    assertTrue(p.multiplicity().isRequired(), "lock field " + k + " is required");
                }
            }
        }
    }

    /** Pure {@code trading::x::Y} is generated as {@code io.concert.trading.x.Y} (see TradingModels). */
    private static Class<?> generated(String qualifiedPureName) {
        try {
            return Class.forName("io.concert." + qualifiedPureName.replace("::", "."));
        } catch (ClassNotFoundException e) {
            throw new AssertionError("no generated class for " + qualifiedPureName, e);
        }
    }

    @Test
    void everyPureTypeHasAGeneratedClass() {
        model.classes().forEach(c -> assertTrue(ModelObject.class.isAssignableFrom(generated(c.qualifiedName())), c.qualifiedName()));
        model.enums().forEach(e -> assertEquals(enumValues(e.qualifiedName()),
                names((Enum<?>[]) generated(e.qualifiedName()).getEnumConstants()), e.qualifiedName()));
    }

    @Test
    void flowsReferenceGeneratedPayloadAndModelClasses() {
        for (TradingFlows.Flow f : TradingFlows.all()) {
            assertTrue(ModelObject.class.isAssignableFrom(generated(TradingFlows.ORDER_PACKAGE + f.modelClass())));
            Class<?> status = generated(TradingFlows.ORDER_PACKAGE + f.statusEnum());
            assertEquals(f.states(), Set.copyOf(names((Enum<?>[]) status.getEnumConstants())), f.smType());
            for (TradingFlows.Transition t : f.transitions()) {
                assertTrue(OrderEvent.class.isAssignableFrom(generated(t.qualifiedPayloadClass())), t.toString());
            }
        }
    }
}
