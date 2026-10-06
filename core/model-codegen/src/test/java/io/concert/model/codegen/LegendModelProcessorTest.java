package io.concert.model.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.tools.Diagnostic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LegendModelProcessorTest {

    private static final String HANDLER = """
            package app;

            import io.concert.model.runtime.LegendModel;

            @LegendModel(file = "order.pure", root = "demo::order::Order")
            public class OrderHandler {}
            """;

    /** Uses generated types in the same compilation as the handler that triggers their generation. */
    private static final String USER = """
            package app;

            import app.model.demo.order.Order;
            import app.model.demo.order.OrderLine;
            import io.concert.model.runtime.ModelJson;

            public class OrderService {
                public String create() {
                    Order order = new Order().setId("o-1").addLine(new OrderLine().setSku("A"));
                    return ModelJson.write(order);
                }
            }
            """;

    @TempDir
    Path work;

    private Path models() throws IOException {
        Path dir = Files.createDirectories(work.resolve("models"));
        Files.copy(JavaGeneratorTest.fixture("order.pure"), dir.resolve("order.pure"));
        return dir;
    }

    private TestCompiler.Result compile(Map<String, String> sources, String... extraOptions) throws IOException {
        Path src = Files.createDirectories(work.resolve("src"));
        List<Path> files = new ArrayList<>();
        for (Map.Entry<String, String> e : sources.entrySet()) {
            Path file = src.resolve(e.getKey());
            Files.createDirectories(file.getParent());
            Files.writeString(file, e.getValue());
            files.add(file);
        }
        List<String> options = new ArrayList<>(List.of("-Xlint:all,-processing", "-Werror", "-proc:full"));
        options.addAll(List.of(extraOptions));
        return TestCompiler.compile(work, files, options, List.of(new LegendModelProcessor()));
    }

    private static String errors(TestCompiler.Result result) {
        return String.join("\n", result.messages(Diagnostic.Kind.ERROR));
    }

    @Test
    void generatesModelUsableInTheSameCompilation() throws IOException {
        Path models = models();
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER, "app/OrderService.java", USER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + work.resolve("missing") + "," + models);
        assertTrue(result.success(), result.report());
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/order/Order.java")));
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/OrderModel.java")));
        assertTrue(Files.exists(result.classes().resolve("app/model/demo/order/OrderLine.class")));
        assertTrue(Files.exists(result.classes().resolve("app/OrderService.class")));
        // The many-to-many association warning is a note, so -Werror does not fail the build.
        assertTrue(result.messages(Diagnostic.Kind.NOTE).stream().anyMatch(m -> m.contains("order.pure:") && m.contains("many-to-many")),
                result.report());
    }

    @Test
    void handlersSharingAFileGenerateItOnce() throws IOException {
        Path models = models();
        String second = """
                package app;

                @io.concert.model.runtime.LegendModel(file = "order.pure", root = "demo::order::Shipment")
                public class ShipmentHandler {}
                """;
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER, "app/ShipmentHandler.java", second),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + models);
        assertTrue(result.success(), result.report());
    }

    @Test
    void sameFileInTwoPackagesIsAnError() throws IOException {
        Path models = models();
        String other = """
                package app;

                @io.concert.model.runtime.LegendModel(file = "order.pure", root = "demo::order::Order", javaPackage = "other")
                public class OtherHandler {}
                """;
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER, "app/OtherHandler.java", other),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + models);
        assertFalse(result.success());
        assertTrue(errors(result).contains("must use the same javaPackage"), result.report());
    }

    @Test
    void missingFileListsSearchedDirectories() throws IOException {
        Path empty = Files.createDirectories(work.resolve("empty"));
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + empty);
        assertFalse(result.success());
        assertEquals(List.of("model file order.pure not found in concert.modelRoots directories: " + empty.toAbsolutePath().normalize()),
                result.messages(Diagnostic.Kind.ERROR));
        assertEquals("OrderHandler.java", Path.of(result.diagnostics().getFirst().getSource().getName()).getFileName().toString());
    }

    @Test
    void missingOptionIsAnError() throws IOException {
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER));
        assertFalse(result.success());
        assertTrue(errors(result).contains("-Aconcert.modelRoots is not set"), result.report());
    }

    @Test
    void parseErrorKeepsModelLocation() throws IOException {
        Path dir = Files.createDirectories(work.resolve("models"));
        Files.writeString(dir.resolve("order.pure"), """
                Class demo::order::Order
                {
                  id: String[1]
                }
                """);
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertFalse(result.success());
        assertEquals(List.of("order.pure:4:1: expected ';' but found '}'"), result.messages(Diagnostic.Kind.ERROR));
    }

    @Test
    void resolverErrorsAreReportedEach() throws IOException {
        Path dir = Files.createDirectories(work.resolve("models"));
        Files.writeString(dir.resolve("order.pure"), "Class demo::order::Order { a: Nope[1]; b: Gone[1]; }\n");
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertFalse(result.success());
        assertEquals(List.of("order.pure:1:28: unknown type 'Nope' for property 'a' of demo::order::Order",
                        "order.pure:1:40: unknown type 'Gone' for property 'b' of demo::order::Order"),
                result.messages(Diagnostic.Kind.ERROR));
    }

    @Test
    void unknownRootClass() throws IOException {
        Path models = models();
        TestCompiler.Result result = compile(Map.of("app/OrderHandler.java", HANDLER.replace("demo::order::Order\"", "demo::order::Nope\"")),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + models);
        assertFalse(result.success());
        assertTrue(errors(result).startsWith("root demo::order::Nope is not a class in order.pure; classes: demo::order::Money,"),
                result.report());
    }

    // ---- adders ----

    @Test
    void adderNamesUseSingularsAndFallBackOnCollision() throws IOException {
        Path dir = Files.createDirectories(work.resolve("models"));
        Files.writeString(dir.resolve("team.pure"), """
                Class demo::Person { name: String[1]; }
                Class demo::Team
                {
                  people: Person[*];
                  children: Person[*];
                  categories: String[*];
                  addresses: String[*];
                  boxes: String[*];
                  statuses: String[*];
                  item: String[0..1];
                  items: String[*];
                }
                """);
        String handler = """
                package app;

                import app.model.demo.Person;
                import app.model.demo.Team;

                @io.concert.model.runtime.LegendModel(file = "team.pure", root = "demo::Team")
                public class TeamHandler {
                    Team team() {
                        return new Team().addPerson(new Person()).addChild(new Person()).addCategory("c").addAddress("a")
                                .addBox("b").addStatus("s").addToItems("i");
                    }
                }
                """;
        TestCompiler.Result result = compile(Map.of("app/TeamHandler.java", handler), "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertTrue(result.success(), result.report());
    }

    // ---- file sets ----

    private static final String COMMON = """
            Enum demo::common::Currency { EUR, USD }
            Class demo::common::Money { amount: Decimal[1]; currency: Currency[1]; }
            """;

    private Path fileSetModels(String paymentModel) throws IOException {
        Path dir = Files.createDirectories(work.resolve("models"));
        Files.writeString(dir.resolve("common.pure"), COMMON);
        Files.writeString(dir.resolve("sale.pure"), """
                import demo::common::*;
                Class demo::sale::Sale { total: Money[1]; }
                """);
        Files.writeString(dir.resolve("payment.pure"), paymentModel);
        return dir;
    }

    private static final String SALE_HANDLER = """
            package app;

            import app.model.demo.common.Currency;
            import app.model.demo.common.Money;
            import app.model.demo.sale.Sale;

            @io.concert.model.runtime.LegendModel(files = {"common.pure", "sale.pure"}, root = "demo::sale::Sale")
            public class SaleHandler {
                Sale sale() {
                    return new Sale().setTotal(new Money().setAmount(java.math.BigDecimal.ONE).setCurrency(Currency.EUR));
                }
            }
            """;

    private static final String PAYMENT_HANDLER = """
            package app;

            import app.model.demo.common.Money;
            import app.model.demo.payment.Payment;

            @io.concert.model.runtime.LegendModel(files = {"common.pure", "payment.pure"}, root = "demo::payment::Payment")
            public class PaymentHandler {
                Money amount(Payment p) {
                    return p.getAmount();
                }
            }
            """;

    @Test
    void fileSetsShareACommonFile() throws IOException {
        Path dir = fileSetModels("""
                import demo::common::*;
                Class demo::payment::Payment { amount: Money[1]; }
                """);
        TestCompiler.Result result = compile(Map.of("app/SaleHandler.java", SALE_HANDLER, "app/PaymentHandler.java", PAYMENT_HANDLER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertTrue(result.success(), result.report());
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/common/Money.java")));
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/CommonModel.java")));
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/SaleModel.java")));
        assertTrue(Files.exists(result.generated().resolve("app/model/demo/PaymentModel.java")));
    }

    @Test
    void sharedFileGeneratedDifferentlyIsAnError() throws IOException {
        // The association adds a back-reference to Money that the other set does not have.
        Path dir = fileSetModels("""
                import demo::common::*;
                Class demo::payment::Payment { id: String[1]; }
                Association demo::payment::PaymentAmount { payment: Payment[0..1]; amount: Money[1]; }
                """);
        TestCompiler.Result result = compile(Map.of("app/SaleHandler.java", SALE_HANDLER, "app/PaymentHandler.java", PAYMENT_HANDLER),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertFalse(result.success());
        assertTrue(errors(result).contains("app.model.demo.common.Money is generated differently"), result.report());
    }

    @Test
    void sharedFileInTwoPackagesIsAnError() throws IOException {
        Path dir = fileSetModels("""
                import demo::common::*;
                Class demo::payment::Payment { amount: Money[1]; }
                """);
        String other = """
                package app;

                @io.concert.model.runtime.LegendModel(files = {"payment.pure", "common.pure"}, root = "demo::payment::Payment",
                        javaPackage = "other")
                public class PaymentHandler {}
                """;
        TestCompiler.Result result = compile(Map.of("app/SaleHandler.java", SALE_HANDLER, "app/PaymentHandler.java", other),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + dir);
        assertFalse(result.success());
        assertTrue(errors(result).contains("model file common.pure is already generated into package"), result.report());
    }

    @Test
    void exactlyOneOfFileAndFiles() throws IOException {
        Path models = models();
        String both = """
                package app;

                @io.concert.model.runtime.LegendModel(file = "order.pure", files = {"order.pure"}, root = "demo::order::Order")
                public class Both {}
                """;
        String neither = """
                package app;

                @io.concert.model.runtime.LegendModel(root = "demo::order::Order")
                public class Neither {}
                """;
        TestCompiler.Result result = compile(Map.of("app/Both.java", both, "app/Neither.java", neither),
                "-A" + LegendModelProcessor.MODEL_ROOTS + "=" + models);
        assertFalse(result.success());
        assertEquals(List.of("@LegendModel must set exactly one of file and files",
                        "@LegendModel must set exactly one of file and files, not both"),
                result.messages(Diagnostic.Kind.ERROR).stream().sorted().toList());
    }
}
