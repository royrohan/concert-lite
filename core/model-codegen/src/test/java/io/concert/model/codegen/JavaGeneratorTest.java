package io.concert.model.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.javapoet.JavaFile;
import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.model.runtime.PureStereotype;
import io.concert.model.runtime.PureTag;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.tools.Diagnostic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JavaGeneratorTest {

    private static final String PKG = "gen.demo.order.";

    @TempDir
    static Path work;

    private static Map<String, String> sources;
    private static URLClassLoader loader;

    static Path fixture(String name) {
        try {
            return Path.of(JavaGeneratorTest.class.getResource("/models/" + name).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeAll
    static void generateAndCompile() throws IOException {
        ResolvedModel model = new ModelResolver().resolve(PureParser.parse(fixture("order.pure")));
        List<JavaFile> files = new JavaGenerator().generate(model, "gen");
        Path src = Files.createDirectories(work.resolve("src"));
        for (JavaFile f : files) {
            f.writeTo(src);
        }
        sources = files.stream().collect(Collectors.toMap(f -> f.packageName() + "." + f.typeSpec().name(), JavaFile::toString));
        TestCompiler.Result result = TestCompiler.compile(work, TestCompiler.javaSources(src), List.of("-Xlint:all", "-Werror"), List.of());
        assertTrue(result.success(), result.report());
        assertEquals(List.of(), result.messages(Diagnostic.Kind.WARNING), result.report());
        loader = result.classLoader();
    }

    @AfterAll
    static void close() throws IOException {
        loader.close();
    }

    // ---- reflection helpers ----

    private static Class<?> type(String simpleName) {
        try {
            return loader.loadClass(simpleName.contains(".") ? simpleName : PKG + simpleName);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    private static ModelObject create(String simpleName) {
        try {
            return (ModelObject) type(simpleName).getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Method method(Object target, String name) {
        return Arrays.stream(target.getClass().getMethods())
                .filter(m -> m.getName().equals(name))
                .min((a, b) -> Boolean.compare(a.isBridge(), b.isBridge()))
                .orElseThrow(() -> new AssertionError("no method " + name + " on " + target.getClass()));
    }

    private static Object call(Object target, String name, Object... args) {
        try {
            return method(target, name).invoke(target, args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Object enumValue(String simpleName, String constant) {
        return Arrays.stream(type(simpleName).getEnumConstants())
                .filter(c -> ((Enum<?>) c).name().equals(constant))
                .findFirst()
                .orElseThrow();
    }

    private static Object money(String amount) {
        return call(create("Money"), "setAmount", new BigDecimal(amount));
    }

    private static ModelObject line(String sku) {
        return (ModelObject) call(create("OrderLine"), "setSku", sku);
    }

    private static ModelObject validOrder() {
        ModelObject order = create("Order");
        call(order, "setId", "o-1");
        call(order, "setTotal", money("12.50"));
        call(order, "addLine", line("A"));
        call(order, "addLine", call(line("B"), "setQuantity", 2L));
        call(order, "setShipment", call(create("Shipment"), "setCarrier", "dhl"));
        call(order, "setInvoice", call(create("Invoice"), "setNumber", "inv-1"));
        call(order, "setCreatedAt", Instant.parse("2024-05-01T10:00:00Z"));
        call(order, "setDefault", "d");
        return order;
    }

    // ---- tests ----

    @Test
    void defaultsOfEveryKindAreApplied() {
        ModelObject order = create("Order");
        assertEquals(enumValue("OrderStatus", "NEW"), call(order, "getStatus"));
        assertEquals(List.of(enumValue("Channel", "web"), enumValue("Channel", "new_")), call(order, "getChannels"));
        assertEquals(false, call(order, "getExpress"));
        assertEquals(3L, call(order, "getPriority"));
        assertEquals(1.5, call(order, "getWeight"));
        assertEquals(new BigDecimal("0.25"), call(order, "getDiscount"));
        assertEquals(new BigDecimal("7"), call(order, "getScore"));
        assertEquals(List.of("web", "new"), call(order, "getLabels"));
        assertEquals(List.of(1L, 2L), call(order, "getPriorities"));
        assertEquals("system", call(order, "getCreatedBy"));
        assertEquals(List.of(), call(order, "getLines"));
        assertNull(call(order, "getTotal"));
        assertEquals(enumValue("Currency", "EUR"), call(create("Money"), "getCurrency"));
        assertEquals(1L, call(create("OrderLine"), "getQuantity"));
    }

    @Test
    void jsonNestsOwnedChildrenAndOmitsBackReferences() throws IOException {
        ModelObject order = validOrder();
        call(order, "addTag", call(create("Tag"), "setName", "t"));
        String json = ModelJson.write(order);
        assertEquals(json, ModelJson.write(order));
        assertEquals(ModelJson.write(validOrder()), ModelJson.write(validOrder()));
        JsonNode tree = new ObjectMapper().readTree(json);
        assertEquals(2, tree.get("lines").size());
        assertEquals("A", tree.get("lines").get(0).get("sku").asText());
        assertFalse(tree.get("lines").get(0).has("order"), json);
        assertEquals("dhl", tree.get("shipment").get("carrier").asText());
        assertFalse(tree.get("shipment").has("order"), json);
        assertFalse(tree.has("tags"), json);
        assertEquals("demo::order::Order", tree.get("@type").asText(), "Entity has subclasses, so its hierarchy carries type ids");
        assertEquals("d", tree.get("default").asText());
        assertEquals("[\"web\",\"new\"]", tree.get("channels").toString());
        assertTrue(json.contains("\"total\":{\"amount\":12.50,\"currency\":\"EUR\"}"), json);
        assertEquals("2024-05-01T10:00:00Z", tree.get("createdAt").asText());
        List<String> keys = tree.properties().stream().map(Map.Entry::getKey).toList();
        assertEquals(keys.stream().sorted().toList(), keys);
    }

    @Test
    void readRelinksBackReferences() {
        ModelObject order = validOrder();
        ModelObject copy = ModelJson.read(ModelJson.write(order), type("Order").asSubclass(ModelObject.class));
        assertEquals(order, copy);
        assertEquals(order.hashCode(), copy.hashCode());
        List<?> lines = (List<?>) call(copy, "getLines");
        assertEquals(2, lines.size());
        for (Object line : lines) {
            assertSame(copy, call(line, "getOrder"));
        }
        assertSame(copy, call(call(copy, "getShipment"), "getOrder"));
        assertSame(copy, call(call(copy, "getInvoice"), "getOrder"));
        assertEquals(ModelJson.write(order), ModelJson.write(copy));
    }

    @Test
    void adderSetsBackReference() {
        ModelObject order = create("Order");
        ModelObject line = line("A");
        call(order, "addLine", line);
        assertSame(order, call(line, "getOrder"));
    }

    @Test
    void validationReportsPathQualifiedErrors() {
        assertEquals(List.of(), validOrder().validationErrors());
        ModelObject order = create("Order");
        call(order, "setPriorities", List.of());
        call(order, "setTotal", call(create("Money"), "setCurrency", (Object) null));
        call(order, "addLine", line("A"));
        call(order, "addLine", create("OrderLine"));
        call(order, "setShipment", call(create("Shipment"), "setTrackingIds", List.of("1", "2", "3", "4")));
        assertEquals(List.of(
                        "Order.id: required [1]",
                        "Order.priorities: expected [1..*] values but found 0",
                        "Order.total.amount: required [1]",
                        "Order.total.currency: required [1]",
                        "Order.lines[1].sku: required [1]",
                        "Order.shipment.carrier: required [1]",
                        "Order.shipment.trackingIds: expected [0..3] values but found 4"),
                order.validationErrors());
        assertEquals(List.of("Order.id: required [1]", "Order.total: required [1]",
                        "Order.lines: expected [1..*] values but found 0"),
                create("Order").validationErrors());
    }

    @Test
    void polymorphicPropertyRoundTripsWithTypeId() throws IOException {
        ModelObject order = validOrder();
        // setReference is inherited from Payment; the covariant override keeps the chain typed.
        Object card = call(call(create("CardPayment"), "setReference", "r-1"), "setLast4", "4242");
        call(order, "setPayment", card);
        String json = ModelJson.write(order);
        JsonNode payment = new ObjectMapper().readTree(json).get("payment");
        assertEquals("demo::order::CardPayment", payment.get("@type").asText());
        assertEquals("4242", payment.get("last4").asText());
        ModelObject copy = ModelJson.read(json, type("Order").asSubclass(ModelObject.class));
        Object paymentCopy = call(copy, "getPayment");
        assertEquals(type("CardPayment"), paymentCopy.getClass());
        assertEquals("r-1", call(paymentCopy, "getReference"));
        assertEquals(card, paymentCopy);

        ModelObject bad = validOrder();
        call(bad, "setPayment", create("WalletPayment"));
        assertEquals(List.of("Order.payment.reference: required [1]", "Order.payment.wallet: required [1]"),
                bad.validationErrors());
    }

    @Test
    void inheritanceExtendsFirstSupertypeAndFlattensTheRest() throws NoSuchFieldException {
        Class<?> order = type("Order");
        assertEquals(type("Entity"), order.getSuperclass());
        assertNotNull(order.getDeclaredField("createdAt"));
        assertEquals(type("Payment"), type("CardPayment").getSuperclass());
        assertTrue(ModelObject.class.isAssignableFrom(type("Payment")));
    }

    @Test
    void stereotypesTagsAndIgnoredEndsAreAnnotated() throws NoSuchFieldException {
        Class<?> order = type("Order");
        assertEquals("demo::meta::Governance.aggregate", order.getAnnotation(PureStereotype.class).value());
        PureTag owner = order.getAnnotation(PureTag.class);
        assertEquals(List.of("demo::meta::Governance", "owner", "team-orders"), List.of(owner.profile(), owner.tag(), owner.value()));
        assertEquals("demo::meta::Governance.pii",
                order.getDeclaredField("customerEmail").getAnnotation(PureStereotype.class).value());
        assertEquals("2024", order.getDeclaredField("customerEmail").getAnnotation(PureTag.class).value());
        assertNotNull(type("OrderLine").getDeclaredField("order").getAnnotation(JsonIgnore.class));
        assertNotNull(order.getDeclaredField("tags").getAnnotation(JsonIgnore.class));
        assertNotNull(type("Tag").getDeclaredField("orders").getAnnotation(JsonIgnore.class));
        assertNull(order.getDeclaredField("lines").getAnnotation(JsonIgnore.class));
    }

    @Test
    void derivedPropertiesAndConstraintsAreDocumented() {
        String order = sources.get(PKG + "Order");
        assertTrue(order.contains("<li>Derived: <code>lineCount() : Integer[1] = $this.lines-&gt;size()</code>"), order);
        assertTrue(order.contains("<li>Constraint hasLines: <code>$this.lines-&gt;size() &gt; 0</code>"), order);
        assertTrue(order.contains("@Generated(\"io.concert.model.codegen\")"), order);
    }

    @Test
    void modelClassHasMermaidDiagramAndClassList() throws ReflectiveOperationException {
        // Registry class: base package + the first Pure package segment of the file's types (demo::order::*).
        Class<?> model = type("gen.demo.OrderModel");
        String mermaid = (String) model.getField("MERMAID").get(null);
        assertTrue(mermaid.startsWith("classDiagram\n"), mermaid);
        assertTrue(mermaid.contains("  Entity <|-- Order\n  Auditable <|-- Order\n"), mermaid);
        assertTrue(mermaid.contains("  Order \"1\" *-- \"1..*\" OrderLine : lines\n"), mermaid);
        assertTrue(mermaid.contains("  Order \"1\" *-- \"0..1\" Shipment : shipment\n"), mermaid);
        assertTrue(mermaid.contains("  Order \"*\" -- \"*\" Tag : orders / tags\n"), mermaid);
        assertTrue(mermaid.contains("    <<enumeration>>\n    EUR\n"), mermaid);
        assertTrue(mermaid.contains("    +lineCount() Integer[1]\n"), mermaid);
        List<?> classes = (List<?>) model.getField("CLASSES").get(null);
        assertTrue(classes.contains(type("Order")));
        assertTrue(classes.contains(type("Currency")));
        assertEquals(14, classes.size());
    }

    @Test
    void modelClassFallsBackToTheBasePackageWhenFirstSegmentsDiffer() {
        ResolvedModel mixed = new ModelResolver().resolve(PureParser.parse("""
                Class a::x::A { name: String[1]; }
                Class b::y::B { name: String[1]; }
                """, "mixed.pure"));
        List<String> names = new JavaGenerator().generate(mixed, "gen").stream()
                .map(f -> f.packageName() + "." + f.typeSpec().name()).toList();
        assertTrue(names.contains("gen.MixedModel"), names.toString());
        assertTrue(names.contains("gen.a.x.A"), names.toString());
    }
}
