package io.concert.model.pure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.model.pure.Literal.EnumLit;
import java.util.List;
import org.junit.jupiter.api.Test;

class ModelResolverTest {

    private final ResolvedModel shop = Fixtures.shop();

    private ClassDef cls(String name) {
        return shop.findClass(name).orElseThrow();
    }

    private static PropertyDef prop(ClassDef c, String name) {
        return c.properties().stream().filter(p -> p.name().equals(name)).findFirst().orElseThrow();
    }

    private static List<String> resolveErrors(String... sources) {
        PureModel[] models = new PureModel[sources.length];
        for (int i = 0; i < sources.length; i++) {
            models[i] = PureParser.parse(sources[i], "t" + (i == 0 ? "" : String.valueOf(i)) + ".pure");
        }
        return assertThrows(PureModelException.class, () -> new ModelResolver().resolve(models)).errors();
    }

    private static ResolvedModel resolve(String source) {
        return new ModelResolver().resolve(PureParser.parse(source, "t.pure"));
    }

    // ---- fixture resolution ----

    @Test
    void typesAndSupertypesAreQualified() {
        ClassDef order = cls("shop::domain::Order");
        assertEquals(List.of("shop::domain::Auditable"), order.superTypes());
        assertEquals(new TypeRef("shop::domain::OrderStatus", false), prop(order, "status").type());
        assertEquals(new TypeRef("shop::domain::Address", false), prop(order, "shippingAddress").type());
        assertEquals(new TypeRef("String", true), prop(order, "orderId").type());
        assertTrue(shop.isEnum(prop(order, "status").type()));
        assertTrue(shop.isClass(prop(order, "shippingAddress").type()));
        assertFalse(shop.isClass(prop(order, "orderId").type()));
    }

    @Test
    void enumDefaultsAreQualified() {
        assertEquals(new EnumLit("shop::domain::Currency", "EUR"), prop(cls("shop::domain::Order"), "currency").defaultValue());
        assertEquals(new EnumLit("shop::domain::Currency", "USD"),
                prop(cls("shop::events::OrderPlaced"), "currency").defaultValue());
    }

    @Test
    void crossFileReferencesResolve() {
        ClassDef placed = cls("shop::events::OrderPlaced");
        assertEquals(new TypeRef("shop::domain::Order", false), prop(placed, "order").type());
        assertEquals(new TypeRef("shop::domain::Customer", false), prop(placed, "customer").type());
        assertEquals(11, shop.classes().size());
        assertEquals(2, shop.functions().size());
    }

    @Test
    void allPropertiesListsSupertypesFirst() {
        assertEquals(List.of("name", "emails", "createdAt", "createdBy", "loyaltyPoints", "vip"),
                shop.allProperties(cls("shop::domain::Customer")).stream().map(PropertyDef::name).toList());
        assertEquals("createdAt", shop.allProperties(cls("shop::domain::Order")).getFirst().name());
    }

    @Test
    void oneToManyOwnsTheManySide() {
        List<AssociationEnd> orderEnds = shop.associationProperties(cls("shop::domain::Order"));
        assertEquals(List.of("lines", "shipment"), orderEnds.stream().map(e -> e.navigable().name()).toList());
        AssociationEnd lines = orderEnds.get(0);
        assertEquals(new TypeRef("shop::domain::OrderLine", false), lines.navigable().type());
        assertEquals("order", lines.opposite().name());
        assertTrue(lines.owned());

        AssociationEnd back = shop.associationProperties(cls("shop::domain::OrderLine")).getFirst();
        assertEquals("order", back.navigable().name());
        assertEquals(new TypeRef("shop::domain::Order", false), back.navigable().type());
        assertFalse(back.owned());
    }

    @Test
    void oneToOneOwnsCompositeEnd() {
        AssociationEnd shipment = shop.associationProperties(cls("shop::domain::Order")).get(1);
        assertEquals("shop::domain::OrderShipment", shipment.association().qualifiedName());
        assertTrue(shipment.owned());
        assertFalse(shop.associationProperties(cls("shop::domain::Shipment")).getFirst().owned());
    }

    @Test
    void oneToOneWithoutCompositeOwnsEnd2() {
        AssociationEnd cart = shop.associationProperties(cls("shop::domain::Customer")).getFirst();
        assertEquals("cart", cart.navigable().name());
        assertTrue(cart.owned());
        assertFalse(shop.associationProperties(cls("shop::events::Cart")).getFirst().owned());
    }

    @Test
    void manyToManyOwnsNeitherAndWarns() {
        assertFalse(shop.associationProperties(cls("shop::domain::Product")).getFirst().owned());
        assertFalse(shop.associationProperties(cls("shop::domain::Category")).getFirst().owned());
        assertEquals(List.of("shop.pure:123:49: association shop::domain::ProductCategories is many-to-many; "
                + "neither end is owned for serialization"), shop.warnings());
    }

    @Test
    void associationPropertiesAreInherited() {
        ResolvedModel m = resolve("""
                Class a::Base {}
                Class a::Sub extends a::Base {}
                Class a::Note {}
                Association a::BaseNotes { owner: a::Base[1]; notes: a::Note[*]; }
                """);
        assertEquals(List.of("notes"), m.associationProperties(m.findClass("a::Sub").orElseThrow()).stream()
                .map(e -> e.navigable().name()).toList());
    }

    @Test
    void diamondInheritanceListsEachPropertyOnce() {
        ResolvedModel m = resolve("""
                Class a::Root { id: String[1]; }
                Class a::Left extends a::Root { l: String[1]; }
                Class a::Right extends a::Root { r: String[1]; }
                Class a::Leaf extends a::Left, a::Right { z: String[1]; }
                """);
        assertEquals(List.of("id", "l", "r", "z"),
                m.allProperties(m.findClass("a::Leaf").orElseThrow()).stream().map(PropertyDef::name).toList());
    }

    @Test
    void undefinedProfileIsAllowed() {
        ResolvedModel m = resolve("Class <<ext::Profile.anything>> {ext::Profile.tag = 'v'} a::A { x: String[1]; }");
        assertEquals(1, m.classes().size());
    }

    @Test
    void importsDisambiguateUnqualifiedNames() {
        ResolvedModel m = resolve("""
                import b::*;
                Enum a::Kind { X }
                Enum b::Kind { Y }
                Class a::Item {}
                Class b::Item {}
                Class c::Base {}
                Class c::Holder extends Base { item: Item[1]; kind: Kind[1] = Kind.Y; }
                """);
        ClassDef holder = m.findClass("c::Holder").orElseThrow();
        assertEquals(new TypeRef("b::Item", false), prop(holder, "item").type());
        assertEquals(new EnumLit("b::Kind", "Y"), prop(holder, "kind").defaultValue());
        assertEquals(List.of("c::Base"), holder.superTypes());
    }

    @Test
    void importsAreScopedToTheirFile() {
        List<String> errors = resolveErrors("""
                Class a::Item {}
                Class b::Item {}
                """, """
                import a::*;
                Class c::Ok { item: Item[1]; }
                """, """
                Class c::Bad { item: Item[1]; }
                """);
        assertEquals(List.of("t2.pure:1:16: ambiguous type 'Item' for property 'item' of c::Bad; candidates: a::Item, b::Item"),
                errors);
    }

    @Test
    void ambiguousAmongImports() {
        assertEquals(List.of("t.pure:5:19: ambiguous type 'Item' for property 'item' of c::Holder among imports;"
                        + " candidates: a::Item, b::Item"),
                resolveErrors("""
                import a::*;
                import b::*;
                Class a::Item {}
                Class b::Item {}
                Class c::Holder { item: Item[1]; }
                """));
    }

    @Test
    void profileReferencesAreQualifiedWhenDefined() {
        ResolvedModel m = resolve("""
                import p::*;
                Profile p::Gov { stereotypes: [reviewed]; tags: [owner]; }
                Class <<Gov.reviewed>> {Gov.owner = 'me'} a::A { <<p::Gov.reviewed>> {ext::Other.t = 'v'} x: String[1]; }
                Enum <<Gov.reviewed>> a::E { <<Gov.reviewed>> V }
                Association <<Gov.reviewed>> a::AB { a: a::A[1]; bs: a::B[*]; }
                Class a::B {}
                """);
        ClassDef a = m.findClass("a::A").orElseThrow();
        assertEquals(List.of(new StereotypeRef("p::Gov", "reviewed")), a.stereotypes());
        assertEquals(List.of(new TaggedValue("p::Gov", "owner", "me")), a.tags());
        assertEquals(List.of(new StereotypeRef("p::Gov", "reviewed")), prop(a, "x").stereotypes());
        assertEquals(List.of(new TaggedValue("ext::Other", "t", "v")), prop(a, "x").tags());
        EnumDef e = m.findEnum("a::E").orElseThrow();
        assertEquals(List.of(new StereotypeRef("p::Gov", "reviewed")), e.stereotypes());
        assertEquals(List.of(new StereotypeRef("p::Gov", "reviewed")), e.values().getFirst().stereotypes());
        assertEquals(List.of(new StereotypeRef("p::Gov", "reviewed")), m.associations().getFirst().stereotypes());
    }

    @Test
    void unqualifiedExternalProfileIsKeptAsWritten() {
        ResolvedModel m = resolve("Class <<Ext.marker>> a::A {}");
        assertEquals(List.of(new StereotypeRef("Ext", "marker")), m.findClass("a::A").orElseThrow().stereotypes());
    }

    // ---- errors ----

    @Test
    void unknownType() {
        assertEquals(List.of("t.pure:3:3: unknown type 'Foo' for property 'x' of a::A"), resolveErrors("""
                Class a::A
                {
                  x: Foo[1];
                }
                """));
    }

    @Test
    void ambiguousUnqualifiedType() {
        assertEquals(List.of("t.pure:3:19: ambiguous type 'Item' for property 'item' of c::Holder; candidates: a::Item, b::Item"),
                resolveErrors("""
                Class a::Item {}
                Class b::Item {}
                Class c::Holder { item: Item[1]; }
                """));
    }

    @Test
    void duplicateElementAcrossFiles() {
        assertEquals(List.of("t1.pure:1:7: duplicate element 'a::A' (first defined at t.pure:1:7)"),
                resolveErrors("Class a::A {}", "Class a::A {}"));
    }

    @Test
    void duplicatePropertyInClass() {
        assertEquals(List.of("t.pure:4:3: duplicate property 'x' in class a::A; also declared by a::A at t.pure:3:3"),
                resolveErrors("""
                Class a::A
                {
                  x: String[1];
                  x: Integer[1];
                }
                """));
    }

    @Test
    void duplicateInheritedPropertyReportedOnce() {
        assertEquals(List.of("t.pure:2:27: duplicate property 'id' in class a::B; also declared by a::A at t.pure:1:14"),
                resolveErrors("""
                Class a::A { id: String[1]; }
                Class a::B extends a::A { id: String[1]; }
                Class a::C extends a::B {}
                """));
    }

    @Test
    void duplicateWithAssociationProperty() {
        assertEquals(List.of("t.pure:3:49: duplicate property 'lines' in class a::Order; also declared by a::Order at t.pure:1:18"),
                resolveErrors("""
                Class a::Order { lines: String[*]; }
                Class a::Line {}
                Association a::OrderLines { order: a::Order[1]; lines: a::Line[*]; }
                """));
    }

    @Test
    void unknownSupertype() {
        assertEquals(List.of("t.pure:1:7: unknown supertype 'a::Missing' for class a::A"),
                resolveErrors("Class a::A extends a::Missing {}"));
    }

    @Test
    void inheritanceCycle() {
        assertEquals(List.of("t.pure:1:7: inheritance cycle: a::A -> a::B -> a::A"), resolveErrors("""
                Class a::A extends a::B {}
                Class a::B extends a::A {}
                """));
    }

    @Test
    void associationEndMustBeClass() {
        assertEquals(List.of("t.pure:3:34: association end 'status' of a::Bad must refer to a class, found a::Status"),
                resolveErrors("""
                Enum a::Status { A }
                Class a::A {}
                Association a::Bad { a: a::A[1]; status: a::Status[1]; }
                """));
    }

    @Test
    void unknownStereotypeAndTagInDefinedProfile() {
        assertEquals(List.of(
                        "t.pure:2:59: unknown stereotype 'secret' in profile a::P",
                        // a tagged value carries its own location (its string literal)
                        "t.pure:3:18: unknown tag 'colour' in profile a::P"),
                resolveErrors("""
                Profile a::P { stereotypes: [ok]; tags: [owner]; }
                Class <<a::P.ok>> {a::P.owner = 'me'} a::A { <<P.secret>> x: String[1];
                  {a::P.colour = 'red'} y: String[1]; }
                """));
    }

    @Test
    void defaultTypeMismatches() {
        assertEquals(List.of(
                        "t.pure:4:3: default value for property 'count' must be Integer, found String",
                        "t.pure:5:3: default value for property 'price' must be Float, found Integer",
                        "t.pure:6:3: unknown enum value 'PURPLE' for enum a::Colour",
                        "t.pure:7:3: default value for property 'other' must be a value of a::Colour, found a::Size.BIG",
                        "t.pure:8:3: list default value for single-valued property 'one'",
                        "t.pure:9:3: default value for property 'flags' must be Boolean, found Integer"),
                resolveErrors("""
                Enum a::Colour { RED, GREEN }
                Enum a::Size { BIG }
                Class a::A {
                  count: Integer[1] = 'three';
                  price: Float[1] = 3;
                  colour: a::Colour[1] = Colour.PURPLE;
                  other: a::Colour[1] = a::Size.BIG;
                  one: String[1] = ['a'];
                  flags: Boolean[*] = [true, 1];
                }
                """));
    }

    @Test
    void errorsAreCollectedTogether() {
        PureModelException e = assertThrows(PureModelException.class,
                () -> resolve("Class a::A extends Nope { x: Foo[1]; y: Bar[0..1]; }"));
        assertEquals(3, e.errors().size());
        assertEquals(String.join("\n", e.errors()), e.getMessage());
    }
}
