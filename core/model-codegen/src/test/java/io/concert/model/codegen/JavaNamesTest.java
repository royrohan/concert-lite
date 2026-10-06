package io.concert.model.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JavaNamesTest {

    @Test
    void identifiersEscapeKeywordsAndInvalidCharacters() {
        assertEquals("Order", JavaNames.identifier("Order"));
        assertEquals("class_", JavaNames.identifier("class"));
        assertEquals("_1st", JavaNames.identifier("1st"));
        assertEquals("a_b", JavaNames.identifier("a-b"));
    }

    @Test
    void packagesKeepSegmentsAndEscapeKeywords() {
        assertEquals("gen.demo.Order.new_", JavaNames.javaPackage("gen", "demo::Order::new"));
        assertEquals("gen", JavaNames.javaPackage("gen", ""));
    }

    @Test
    void singularsForAdders() {
        assertEquals("line", JavaNames.singular("lines"));
        assertEquals("orderLine", JavaNames.singular("orderLines"));
        assertEquals("category", JavaNames.singular("categories"));
        assertEquals("subCategory", JavaNames.singular("subCategories"));
        assertEquals("address", JavaNames.singular("addresses"));
        assertEquals("match", JavaNames.singular("matches"));
        assertEquals("wish", JavaNames.singular("wishes"));
        assertEquals("box", JavaNames.singular("boxes"));
        assertEquals("person", JavaNames.singular("people"));
        assertEquals("contactPerson", JavaNames.singular("contactPeople"));
        assertEquals("child", JavaNames.singular("children"));
        assertEquals("status", JavaNames.singular("statuses"));
        assertEquals("orderStatus", JavaNames.singular("orderStatuses"));
        assertEquals("status", JavaNames.singular("status"));
        assertEquals("bonus", JavaNames.singular("bonus"));
        assertEquals("access", JavaNames.singular("access"));
        assertEquals("id", JavaNames.singular("ids"));
        assertEquals("data", JavaNames.singular("data"));
    }

    @Test
    void accessorNames() {
        assertEquals("getClass_", JavaNames.getter("class"));
        assertEquals("getDefault", JavaNames.getter("default"));
        assertEquals("setX", JavaNames.setter("x"));
        assertEquals("line", JavaNames.singular("lines"));
        assertEquals("s", JavaNames.singular("s"));
        assertEquals("Order", JavaNames.fileStem("models/order.pure"));
        assertEquals("My_model", JavaNames.fileStem("my-model.pure"));
    }
}
