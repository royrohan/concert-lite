package io.concert.model.pure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.model.pure.Literal.BoolLit;
import io.concert.model.pure.Literal.EnumLit;
import io.concert.model.pure.Literal.FloatLit;
import io.concert.model.pure.Literal.IntLit;
import io.concert.model.pure.Literal.ListLit;
import io.concert.model.pure.Literal.StringLit;
import java.util.List;
import org.junit.jupiter.api.Test;

class PureParserTest {

    private final PureModel shop = Fixtures.parse("shop.pure");

    private ClassDef cls(String name) {
        return shop.findClass(name).orElseThrow();
    }

    private static PropertyDef prop(ClassDef c, String name) {
        return c.properties().stream().filter(p -> p.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void elementCounts() {
        assertEquals(9, shop.classes().size());
        assertEquals(2, shop.enums().size());
        assertEquals(3, shop.associations().size());
        assertEquals(1, shop.profiles().size());
        assertEquals(1, shop.functions().size());
    }

    @Test
    void importsAreRecordedInOrder() {
        PureModel m = PureParser.parse("""
                import a::b::*;
                Class x::A {}
                ###Pure
                import c::*;
                """, "t.pure");
        assertEquals(List.of("a::b", "c"), m.imports());
        assertEquals(1, m.classes().size());
        assertEquals(List.of(), shop.imports());
    }

    @Test
    void profile() {
        ProfileDef p = shop.findProfile("shop::meta::Governance").orElseThrow();
        assertEquals(List.of("reviewed", "deprecated", "restricted"), p.stereotypes());
        assertEquals(List.of("owner", "since", "ticket"), p.tags());
        assertEquals(new SourceLocation("shop.pure", 6, 9), p.location());
    }

    @Test
    void enumWithAnnotatedValuesAndTrailingComma() {
        EnumDef e = shop.findEnum("shop::domain::OrderStatus").orElseThrow();
        assertEquals(List.of("NEW", "PAID", "ON_HOLD", "SHIPPED", "CANCELLED"),
                e.values().stream().map(EnumValueDef::name).toList());
        assertEquals(List.of(new StereotypeRef("shop::meta::Governance", "deprecated")), e.value("ON_HOLD").orElseThrow().stereotypes());
        assertEquals(List.of(new TaggedValue("shop::meta::Governance", "since", "2024-03-01")), e.value("SHIPPED").orElseThrow().tags());
        assertEquals(new SourceLocation("shop.pure", 16, 41), e.value("ON_HOLD").orElseThrow().location());
        assertEquals("OrderStatus", e.simpleName());
        assertEquals("shop::domain", e.packageName());
    }

    @Test
    void classHeaderWithAnnotationsMultipleInheritanceAndConstraints() {
        ClassDef c = cls("shop::domain::Customer");
        assertEquals("shop::domain", c.packageName());
        assertEquals("Customer", c.simpleName());
        assertEquals(List.of("shop::domain::Party", "shop::domain::Auditable"), c.superTypes());
        assertEquals(List.of(new StereotypeRef("shop::meta::Governance", "reviewed")), c.stereotypes());
        assertEquals(List.of(
                        new TaggedValue("shop::meta::Governance", "owner", "team-orders"),
                        new TaggedValue("shop::meta::Governance", "since", "2024-01-15")),
                c.tags());
        assertEquals(List.of(
                        new ConstraintDef("hasName", "($this.name->length() > 0)"),
                        new ConstraintDef(null, "$this.emails->size() <= 5")),
                c.constraints());
        assertEquals(new IntLit(0), prop(c, "loyaltyPoints").defaultValue());
        assertEquals(new BoolLit(false), prop(c, "vip").defaultValue());
    }

    @Test
    void allPrimitiveTypes() {
        ClassDef o = cls("shop::domain::Order");
        assertEquals(new TypeRef("String", true), prop(o, "orderId").type());
        assertEquals(PrimitiveType.FLOAT, prop(o, "total").type().primitiveType());
        assertEquals(PrimitiveType.DECIMAL, prop(o, "discount").type().primitiveType());
        assertEquals(PrimitiveType.INTEGER, prop(o, "itemCount").type().primitiveType());
        assertEquals(PrimitiveType.NUMBER, prop(o, "weight").type().primitiveType());
        assertEquals(PrimitiveType.BOOLEAN, prop(o, "express").type().primitiveType());
        assertEquals(PrimitiveType.STRICT_DATE, prop(o, "orderDate").type().primitiveType());
        assertEquals(PrimitiveType.DATE_TIME, prop(o, "placedAt").type().primitiveType());
        assertEquals(PrimitiveType.DATE, prop(o, "deliveryDate").type().primitiveType());
        assertEquals(new TypeRef("OrderStatus", false), prop(o, "status").type());
    }

    @Test
    void allMultiplicityForms() {
        ClassDef o = cls("shop::domain::Order");
        assertEquals(Multiplicity.ONE, prop(o, "orderId").multiplicity());
        assertEquals(Multiplicity.ZERO_ONE, prop(o, "discount").multiplicity());
        assertEquals(Multiplicity.MANY, prop(o, "labels").multiplicity());
        assertEquals(Multiplicity.ONE_MANY, prop(o, "priorities").multiplicity());
        assertEquals(new Multiplicity(0, 3), prop(o, "couponCodes").multiplicity());
        assertEquals(new Multiplicity(2, 2), prop(o, "trackingIds").multiplicity());
        assertEquals(new Multiplicity(2, null), prop(o, "ratings").multiplicity());
        assertEquals(List.of("[1]", "[0..1]", "[*]", "[1..*]", "[0..3]", "[2]", "[2..*]"),
                List.of("orderId", "discount", "labels", "priorities", "couponCodes", "trackingIds", "ratings").stream()
                        .map(n -> prop(o, n).multiplicity().toString())
                        .toList());
        Multiplicity opt = prop(o, "discount").multiplicity();
        assertTrue(opt.isToOne() && opt.isOptional() && !opt.isRequired() && !opt.isToMany());
        Multiplicity many = prop(o, "priorities").multiplicity();
        assertTrue(many.isToMany() && many.isRequired() && !many.isToOne());
    }

    @Test
    void defaultsOfEveryLiteralKind() {
        ClassDef o = cls("shop::domain::Order");
        assertEquals(new EnumLit("shop::domain::OrderStatus", "NEW"), prop(o, "status").defaultValue());
        assertEquals(new EnumLit("Currency", "EUR"), prop(o, "currency").defaultValue());
        assertEquals(new FloatLit(100.0), prop(o, "total").defaultValue());
        assertEquals(new IntLit(3), prop(o, "itemCount").defaultValue());
        assertEquals(new IntLit(2), prop(o, "weight").defaultValue());
        assertEquals(new BoolLit(true), prop(o, "express").defaultValue());
        assertEquals(new ListLit(List.of(new StringLit("new"), new StringLit("web"))), prop(o, "labels").defaultValue());
        assertEquals(new StringLit("It's fragile"), prop(o, "note").defaultValue());
        assertEquals(new FloatLit(-4.5), prop(o, "temperature").defaultValue());
        assertEquals(new ListLit(List.of(new IntLit(1), new IntLit(2))), prop(o, "priorities").defaultValue());
        assertNull(prop(o, "discount").defaultValue());
    }

    @Test
    void propertyAnnotationsAndAggregation() {
        ClassDef o = cls("shop::domain::Order");
        PropertyDef internal = prop(o, "internalRef");
        assertEquals(List.of(new StereotypeRef("shop::meta::Governance", "restricted")), internal.stereotypes());
        assertEquals(List.of(new TaggedValue("shop::meta::Governance", "ticket", "SEC-12")), internal.tags());
        assertEquals(new SourceLocation("shop.pure", 72, 84), internal.location());
        assertEquals(Aggregation.COMPOSITE, prop(o, "shippingAddress").aggregation());
        assertEquals(Aggregation.SHARED, prop(o, "billingAddress").aggregation());
        assertEquals(Aggregation.NONE, prop(o, "giftNote").aggregation());
        assertEquals(Aggregation.NONE, prop(o, "orderId").aggregation());
    }

    @Test
    void derivedPropertiesKeepRawBodies() {
        ClassDef o = cls("shop::domain::Order");
        assertEquals(List.of("grandTotal", "label", "lineSkus"), o.derived().stream().map(DerivedPropertyDef::name).toList());
        DerivedPropertyDef grand = o.derived().get(0);
        assertEquals("", grand.params());
        assertEquals("$this.total - if($this.discount->isEmpty(), | 0.0, | $this.discount->toOne())", grand.body());
        assertEquals(new TypeRef("Float", true), grand.returnType());
        assertEquals(Multiplicity.ONE, grand.multiplicity());
        DerivedPropertyDef label = o.derived().get(1);
        assertEquals("prefix: String[1]", label.params());
        assertEquals("$prefix + ' {' + $this.orderId + '}'", label.body());
        DerivedPropertyDef skus = o.derived().get(2);
        assertEquals("$this.lines->map(l | $l.sku)->filter(s | $s != '}')", skus.body());
        assertEquals(Multiplicity.MANY, skus.multiplicity());
    }

    @Test
    void constraintsWithNestedBrackets() {
        assertEquals(List.of(
                        new ConstraintDef("nonNegativeTotal", "($this.total >= 0.0)"),
                        new ConstraintDef("discountWithinTotal", "($this.discount->toOne() <= $this.total)")),
                cls("shop::domain::Order").constraints());
    }

    @Test
    void associations() {
        AssociationDef lines = shop.findAssociation("shop::domain::OrderLines").orElseThrow();
        assertEquals("order", lines.end1().name());
        assertEquals(new TypeRef("Order", false), lines.end1().type());
        assertEquals("lines", lines.end2().name());
        assertEquals(Multiplicity.ONE_MANY, lines.end2().multiplicity());
        AssociationDef shipment = shop.findAssociation("shop::domain::OrderShipment").orElseThrow();
        assertEquals(Aggregation.COMPOSITE, shipment.end1().aggregation());
        AssociationDef cats = shop.findAssociation("shop::domain::ProductCategories").orElseThrow();
        assertEquals(List.of(new StereotypeRef("shop::meta::Governance", "reviewed")), cats.stereotypes());
    }

    @Test
    void functionSignatureAndRawBody() {
        FunctionDef f = shop.findFunctions("shop::domain::orderValue").getFirst();
        assertEquals("o: Order[1], withDiscount: Boolean[1]", f.params());
        assertEquals(new TypeRef("Float", true), f.returnType());
        assertEquals(Multiplicity.ONE, f.multiplicity());
        assertEquals("if($withDiscount, | $o.grandTotal(), | $o.total)", f.body());
    }

    @Test
    void nonPureSectionsAreSkipped() {
        PureModel events = Fixtures.parse("events.pure");
        assertEquals(List.of("shop::events::OrderPlaced", "shop::events::Cart"),
                events.classes().stream().map(ClassDef::qualifiedName).toList());
        assertEquals(1, events.associations().size());
        assertTrue(events.findFunctions("shop::events::isLate").getFirst().body().contains("DurationUnit.DAYS"));
    }

    @Test
    void sectionHeaderIsOptional() {
        PureModel m = PureParser.parse("Class A { x: Integer[1]; }", "t.pure");
        assertEquals("A", m.classes().getFirst().qualifiedName());
        assertEquals("", m.classes().getFirst().packageName());
    }

    @Test
    void taggedValuesKnowWhereTheirLiteralStarts() {
        PureModel m = PureParser.parse("Class {p::P.t = 'v1',\n  p::P.u = 'v2'} a::A { x: String[1]; }", "loc.pure");
        ClassDef a = m.classes().getFirst();
        assertEquals(new SourceLocation("loc.pure", 1, 17), a.tags().get(0).location());
        assertEquals(new SourceLocation("loc.pure", 2, 12), a.tags().get(1).location());
        // location is not part of equality
        assertEquals(new TaggedValue("p::P", "t", "v1"), a.tags().get(0));
    }
}
