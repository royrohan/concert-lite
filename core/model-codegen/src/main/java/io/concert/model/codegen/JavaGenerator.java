package io.concert.model.codegen;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.palantir.javapoet.AnnotationSpec;
import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.CodeBlock;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.JavaFile;
import com.palantir.javapoet.MethodSpec;
import com.palantir.javapoet.ParameterizedTypeName;
import com.palantir.javapoet.TypeName;
import com.palantir.javapoet.TypeSpec;
import com.palantir.javapoet.WildcardTypeName;
import io.concert.model.pure.AssociationDef;
import io.concert.model.pure.AssociationEnd;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ConstraintDef;
import io.concert.model.pure.DerivedPropertyDef;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.EnumValueDef;
import io.concert.model.pure.Literal;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import io.concert.model.pure.StereotypeRef;
import io.concert.model.pure.TaggedValue;
import io.concert.model.pure.TypeRef;
import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.model.runtime.ModelSupport;
import io.concert.model.runtime.PureStereotype;
import io.concert.model.runtime.PureTag;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

/**
 * Generates Java sources for a resolved Pure model: a mutable class implementing {@link ModelObject}
 * per Pure class, a Java enum per Pure enumeration, and a {@code <FileStem>Model} class per source
 * file holding a Mermaid class diagram and the list of generated types. The model class goes into the
 * base package plus the first Pure package segment of the file's types ({@code order.pure} with
 * {@code trading::order::*} under {@code io.concert} gives {@code io.concert.trading.OrderModel}).
 *
 * <p><b>Inheritance.</b> Java allows one superclass, so a generated class extends the Java class of
 * its <em>first</em> Pure supertype; the properties (and association ends) of any further supertypes
 * are flattened into it. A generated class is therefore not a Java subtype of those further
 * supertypes, and a property typed with one of them cannot hold it. Classes with subclasses are
 * serialized with an {@code @type} property holding the Pure qualified name, so polymorphic
 * properties round-trip.
 *
 * <p><b>Associations.</b> The owned end of an association (see {@link ResolvedModel}) is a serialized
 * property; the opposite end is an {@code @JsonIgnore}d back-reference rebuilt by
 * {@link ModelObject#relink()}. Neither end of a many-to-many association is serialized.
 */
public final class JavaGenerator {

    /** Value of the {@code @Generated} annotation on every generated type. */
    public static final String GENERATOR = "io.concert.model.codegen";

    private static final ClassName GENERATED = ClassName.get("javax.annotation.processing", "Generated");
    private static final ClassName LIST = ClassName.get(List.class);
    private static final String INDENT = "    ";

    public List<JavaFile> generate(ResolvedModel model, String basePackage) {
        return new Generation(model, basePackage).run();
    }

    /** Builds a file with the conventions shared by all generated sources. */
    static JavaFile javaFile(String packageName, TypeSpec type) {
        return JavaFile.builder(packageName, type).skipJavaLangImports(true).indent(INDENT).build();
    }

    private enum Kind {
        /** A property declared in a class. */
        STORED,
        /** The owned end of an association: serialized, children get a back-reference. */
        OWNED,
        /** The end opposite an owned end: not serialized, set by {@code relink()}. */
        BACK_REF,
        /** An end of a many-to-many association: not serialized. */
        UNOWNED
    }

    private record Member(PropertyDef property, Kind kind, AssociationEnd end, String adder) {

        String name() {
            return property.name();
        }

        String field() {
            return JavaNames.identifier(name());
        }

        boolean serialized() {
            return kind == Kind.STORED || kind == Kind.OWNED;
        }

        boolean list() {
            return property.multiplicity().isToMany();
        }
    }

    private static final class Generation {

        private final ResolvedModel model;
        private final String basePackage;
        private final Map<String, ClassName> typeNames = new HashMap<>();
        private final Map<String, List<ClassDef>> directSubclasses = new HashMap<>();
        private final Map<String, List<Member>> allMembers = new HashMap<>();
        private final Map<String, List<Member>> ownMembers = new HashMap<>();
        private final Map<String, String> mermaidIds = new HashMap<>();

        Generation(ResolvedModel model, String basePackage) {
            this.model = model;
            this.basePackage = basePackage;
            for (ClassDef c : model.classes()) {
                typeNames.put(c.qualifiedName(), ClassName.get(
                        JavaNames.javaPackage(basePackage, c.packageName()), JavaNames.identifier(c.simpleName())));
                if (!c.superTypes().isEmpty()) {
                    directSubclasses.computeIfAbsent(c.superTypes().getFirst(), k -> new ArrayList<>()).add(c);
                }
            }
            for (EnumDef e : model.enums()) {
                typeNames.put(e.qualifiedName(), ClassName.get(
                        JavaNames.javaPackage(basePackage, e.packageName()), JavaNames.identifier(e.simpleName())));
            }
            Map<String, Long> simpleCounts = typeNames.keySet().stream()
                    .collect(Collectors.groupingBy(Generation::simpleName, Collectors.counting()));
            typeNames.keySet().forEach(q -> mermaidIds.put(q, simpleCounts.get(simpleName(q)) > 1
                    ? q.replace("::", "_")
                    : simpleName(q)));
        }

        List<JavaFile> run() {
            List<JavaFile> files = new ArrayList<>();
            Map<String, List<String>> elementsByFile = new LinkedHashMap<>();
            for (ClassDef c : model.classes()) {
                files.add(javaFile(typeNames.get(c.qualifiedName()).packageName(), classType(c)));
                elementsByFile.computeIfAbsent(c.location().file(), k -> new ArrayList<>()).add(c.qualifiedName());
            }
            for (EnumDef e : model.enums()) {
                files.add(javaFile(typeNames.get(e.qualifiedName()).packageName(), enumType(e)));
                elementsByFile.computeIfAbsent(e.location().file(), k -> new ArrayList<>()).add(e.qualifiedName());
            }
            elementsByFile.forEach((file, elements) -> files.add(javaFile(modelPackage(elements), modelType(file, elements))));
            return files;
        }

        /**
         * Package of a file's {@code <FileStem>Model} class: the base package plus the first Pure package
         * segment its types share ({@code trading::order::Order} under {@code io.concert} gives
         * {@code io.concert.trading}), so registries never land in a bare base package next to unrelated
         * models. Falls back to the base package when the types have no package or differ in the first segment.
         */
        private String modelPackage(List<String> elements) {
            return JavaNames.modelPackage(basePackage, elements);
        }

        // ---- structure ----

        private static String simpleName(String qualifiedName) {
            int i = qualifiedName.lastIndexOf("::");
            return i < 0 ? qualifiedName : qualifiedName.substring(i + 2);
        }

        private ClassDef cls(String qualifiedName) {
            return model.findClass(qualifiedName).orElseThrow();
        }

        /** The class whose Java class this one extends, or {@code null}. */
        private ClassDef javaSuper(ClassDef c) {
            return c.superTypes().isEmpty() ? null : cls(c.superTypes().getFirst());
        }

        private boolean isJavaSubtype(String sub, String sup) {
            for (ClassDef c = cls(sub); c != null; c = javaSuper(c)) {
                if (c.qualifiedName().equals(sup)) {
                    return true;
                }
            }
            return false;
        }

        private List<ClassDef> allJavaSubclasses(ClassDef c) {
            List<ClassDef> result = new ArrayList<>();
            for (ClassDef sub : directSubclasses.getOrDefault(c.qualifiedName(), List.of())) {
                result.add(sub);
                result.addAll(allJavaSubclasses(sub));
            }
            return result;
        }

        /** Own and inherited members of the Java class, superclass members first. */
        private List<Member> allMembers(ClassDef c) {
            List<Member> cached = allMembers.get(c.qualifiedName());
            if (cached != null) {
                return cached;
            }
            ClassDef sup = javaSuper(c);
            List<Member> result = new ArrayList<>(sup == null ? List.of() : allMembers(sup));
            result.addAll(ownMembers(c));
            allMembers.put(c.qualifiedName(), List.copyOf(result));
            return result;
        }

        /** Members declared by the Java class: its own, plus those flattened from additional supertypes. */
        private List<Member> ownMembers(ClassDef c) {
            List<Member> cached = ownMembers.get(c.qualifiedName());
            if (cached != null) {
                return cached;
            }
            ClassDef sup = javaSuper(c);
            List<Member> inherited = sup == null ? List.of() : allMembers(sup);
            Set<String> inheritedClasses = sup == null
                    ? Set.of()
                    : model.linearization(sup).stream().map(ClassDef::qualifiedName).collect(Collectors.toSet());
            List<AssociationEnd> inheritedEnds = sup == null ? List.of() : model.associationProperties(sup);

            record Candidate(PropertyDef property, Kind kind, AssociationEnd end) {}
            List<Candidate> candidates = new ArrayList<>();
            for (ClassDef owner : model.linearization(c)) {
                if (!inheritedClasses.contains(owner.qualifiedName())) {
                    owner.properties().forEach(p -> candidates.add(new Candidate(p, Kind.STORED, null)));
                }
            }
            for (AssociationEnd end : model.associationProperties(c)) {
                if (!inheritedEnds.contains(end)) {
                    candidates.add(new Candidate(end.navigable(), kind(end), end));
                }
            }

            Set<String> names = new HashSet<>();
            inherited.forEach(m -> names.add(m.name()));
            candidates.forEach(m -> names.add(m.property().name()));
            Map<String, Long> naiveAdders = new HashMap<>();
            inherited.stream().filter(Member::list).forEach(m -> naiveAdders.merge(m.adder(), 1L, Long::sum));
            candidates.stream()
                    .filter(m -> m.property().multiplicity().isToMany())
                    .forEach(m -> naiveAdders.merge(naiveAdder(m.property().name()), 1L, Long::sum));

            List<Member> result = new ArrayList<>();
            for (Candidate m : candidates) {
                String adder = null;
                if (m.property().multiplicity().isToMany()) {
                    String name = m.property().name();
                    String singular = JavaNames.singular(name);
                    adder = naiveAdder(name);
                    if ((!singular.equals(name) && names.contains(singular)) || naiveAdders.get(adder) > 1) {
                        adder = JavaNames.identifier("addTo" + JavaNames.capitalize(name));
                    }
                }
                result.add(new Member(m.property(), m.kind(), m.end(), adder));
            }
            ownMembers.put(c.qualifiedName(), List.copyOf(result));
            return result;
        }

        private static String naiveAdder(String property) {
            return JavaNames.identifier("add" + JavaNames.capitalize(JavaNames.singular(property)));
        }

        private static Kind kind(AssociationEnd end) {
            if (end.owned()) {
                return Kind.OWNED;
            }
            return end.navigable().multiplicity().isToMany() && end.opposite().multiplicity().isToMany()
                    ? Kind.UNOWNED
                    : Kind.BACK_REF;
        }

        // ---- types ----

        private TypeName elementType(TypeRef type) {
            if (!type.primitive()) {
                return typeNames.get(type.name());
            }
            return switch (type.primitiveType()) {
                case STRING -> ClassName.get(String.class);
                case INTEGER -> ClassName.get(Long.class);
                case FLOAT -> ClassName.get(Double.class);
                case DECIMAL, NUMBER -> ClassName.get(BigDecimal.class);
                case BOOLEAN -> ClassName.get(Boolean.class);
                case DATE, STRICT_DATE -> ClassName.get(LocalDate.class);
                case DATE_TIME -> ClassName.get(Instant.class);
            };
        }

        private TypeName javaType(Member m) {
            TypeName element = elementType(m.property().type());
            return m.list() ? ParameterizedTypeName.get(LIST, element) : element;
        }

        private CodeBlock literal(Literal value, TypeRef type) {
            boolean decimal = type.primitive() && switch (type.primitiveType()) {
                case DECIMAL, NUMBER -> true;
                default -> false;
            };
            boolean floating = type.primitive() && type.primitiveType() == io.concert.model.pure.PrimitiveType.FLOAT;
            return switch (value) {
                case Literal.StringLit s -> CodeBlock.of("$S", s.value());
                case Literal.IntLit i when decimal -> CodeBlock.of("new $T($S)", BigDecimal.class, Long.toString(i.value()));
                case Literal.IntLit i when floating -> CodeBlock.of("$L", Double.toString(i.value()));
                case Literal.IntLit i -> CodeBlock.of("$LL", i.value());
                case Literal.FloatLit f when decimal -> CodeBlock.of("new $T($S)", BigDecimal.class, Double.toString(f.value()));
                case Literal.FloatLit f -> CodeBlock.of("$L", Double.toString(f.value()));
                case Literal.BoolLit b -> CodeBlock.of("$L", b.value());
                case Literal.EnumLit e -> CodeBlock.of("$T.$L", typeNames.get(e.enumName()), JavaNames.identifier(e.value()));
                case Literal.ListLit l -> l.values().isEmpty()
                        ? CodeBlock.of("new $T<>()", ArrayList.class)
                        : CodeBlock.of("new $T<>($T.of($L))", ArrayList.class, List.class,
                                CodeBlock.join(l.values().stream().map(v -> literal(v, type)).toList(), ", "));
            };
        }

        private CodeBlock initializer(Member m) {
            Literal def = m.property().defaultValue();
            if (m.list()) {
                return def instanceof Literal.ListLit ? literal(def, m.property().type()) : CodeBlock.of("new $T<>()", ArrayList.class);
            }
            return def == null ? null : literal(def, m.property().type());
        }

        // ---- class ----

        private TypeSpec classType(ClassDef c) {
            ClassName self = typeNames.get(c.qualifiedName());
            ClassDef sup = javaSuper(c);
            TypeSpec.Builder type = TypeSpec.classBuilder(self).addModifiers(Modifier.PUBLIC).addAnnotation(generated());
            if (sup == null) {
                type.addSuperinterface(ModelObject.class);
            } else {
                type.superclass(typeNames.get(sup.qualifiedName()));
            }
            type.addJavadoc("$L", classJavadoc(c));
            List<ClassDef> subclasses = allJavaSubclasses(c);
            if (!subclasses.isEmpty()) {
                type.addAnnotation(AnnotationSpec.builder(JsonTypeInfo.class)
                        .addMember("use", "$T.NAME", JsonTypeInfo.Id.class)
                        .addMember("property", "$S", "@type")
                        .build());
                AnnotationSpec.Builder subTypes = AnnotationSpec.builder(JsonSubTypes.class);
                for (ClassDef sub : subclasses) {
                    subTypes.addMember("value", "$L", AnnotationSpec.builder(JsonSubTypes.Type.class)
                            .addMember("value", "$T.class", typeNames.get(sub.qualifiedName()))
                            .addMember("name", "$S", sub.qualifiedName())
                            .build());
                }
                type.addAnnotation(subTypes.build());
            }
            if (!subclasses.isEmpty() || sup != null) {
                type.addAnnotation(AnnotationSpec.builder(JsonTypeName.class).addMember("value", "$S", c.qualifiedName()).build());
            }
            type.addAnnotations(pureAnnotations(c.stereotypes(), c.tags()));

            List<Member> own = ownMembers(c);
            for (Member m : own) {
                type.addField(field(m));
            }
            type.addMethod(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PUBLIC)
                    .addJavadoc("Creates an instance holding the model's default values.\n")
                    .build());
            for (Member m : own) {
                accessors(type, c, m);
            }
            if (sup != null) {
                for (Member m : allMembers(sup)) {
                    covariantOverrides(type, self, m);
                }
            }
            type.addMethod(relink(c, own, sup != null));
            type.addMethod(validation(own, sup != null));
            type.addMethod(equalsMethod(self, own, sup != null));
            type.addMethod(hashCodeMethod(own, sup != null));
            if (sup == null) {
                type.addMethod(MethodSpec.methodBuilder("toString")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(String.class)
                        .addStatement("return getClass().getSimpleName() + $T.write(this)", ModelJson.class)
                        .build());
            }
            return type.build();
        }

        private String classJavadoc(ClassDef c) {
            StringBuilder doc = new StringBuilder("Generated from Pure class <code>" + html(c.qualifiedName()) + "</code> ("
                    + html(c.location().toString()) + ").\n");
            if (c.superTypes().size() > 1) {
                doc.append("\n<p>Also declares the properties of ")
                        .append(c.superTypes().subList(1, c.superTypes().size()).stream()
                                .map(s -> "<code>" + html(s) + "</code>")
                                .collect(Collectors.joining(", ")))
                        .append(", flattened because a Java class has a single superclass.\n");
            }
            if (!c.derived().isEmpty() || !c.constraints().isEmpty()) {
                doc.append("\n<p>Documented from the model, not evaluated:\n<ul>\n");
                for (DerivedPropertyDef d : c.derived()) {
                    doc.append("  <li>Derived: <code>").append(html(d.name() + "(" + d.params() + ") : " + d.returnType()
                            + d.multiplicity() + " = " + d.body())).append("</code>\n");
                }
                for (ConstraintDef k : c.constraints()) {
                    doc.append("  <li>Constraint").append(k.name() == null ? "" : " " + html(k.name())).append(": <code>")
                            .append(html(k.expression())).append("</code>\n");
                }
                doc.append("</ul>\n");
            }
            return doc.toString();
        }

        private FieldSpec field(Member m) {
            FieldSpec.Builder f = FieldSpec.builder(javaType(m), m.field(), Modifier.PRIVATE);
            if (!m.field().equals(m.name())) {
                f.addAnnotation(AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", m.name()).build());
            }
            if (!m.serialized()) {
                f.addAnnotation(JsonIgnore.class);
            }
            f.addAnnotations(pureAnnotations(m.property().stereotypes(), m.property().tags()));
            switch (m.kind()) {
                case BACK_REF -> f.addJavadoc("Back-reference of <code>$L</code>; not serialized, maintained by {@link #relink()}.\n",
                        html(m.end().association().qualifiedName()));
                case UNOWNED -> f.addJavadoc("End of the many-to-many association <code>$L</code>; not serialized, so related "
                        + "objects are not embedded and must be referenced by key.\n", html(m.end().association().qualifiedName()));
                default -> {}
            }
            CodeBlock init = initializer(m);
            if (init != null) {
                f.initializer(init);
            }
            return f.build();
        }

        private void accessors(TypeSpec.Builder type, ClassDef c, Member m) {
            ClassName self = typeNames.get(c.qualifiedName());
            TypeName t = javaType(m);
            type.addMethod(MethodSpec.methodBuilder(JavaNames.getter(m.name()))
                    .addModifiers(Modifier.PUBLIC)
                    .returns(t)
                    .addStatement("return this.$N", m.field())
                    .build());
            type.addMethod(MethodSpec.methodBuilder(JavaNames.setter(m.name()))
                    .addModifiers(Modifier.PUBLIC)
                    .returns(self)
                    .addParameter(t, "value")
                    .addStatement("this.$N = value", m.field())
                    .addStatement("return this")
                    .build());
            if (m.list()) {
                MethodSpec.Builder add = MethodSpec.methodBuilder(m.adder())
                        .addModifiers(Modifier.PUBLIC)
                        .returns(self)
                        .addParameter(elementType(m.property().type()), "item")
                        .beginControlFlow("if (this.$N == null)", m.field())
                        .addStatement("this.$N = new $T<>()", m.field(), ArrayList.class)
                        .endControlFlow()
                        .addStatement("this.$N.add(item)", m.field());
                if (m.kind() == Kind.OWNED) {
                    add.addJavadoc("Adds {@code item} and sets its back-reference to this object.\n");
                    add.addCode(backLink(c, m, "item"));
                }
                type.addMethod(add.addStatement("return this").build());
            }
        }

        private void covariantOverrides(TypeSpec.Builder type, ClassName self, Member m) {
            TypeName t = javaType(m);
            type.addMethod(MethodSpec.methodBuilder(JavaNames.setter(m.name()))
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(self)
                    .addParameter(t, "value")
                    .addStatement("super.$N(value)", JavaNames.setter(m.name()))
                    .addStatement("return this")
                    .build());
            if (m.list()) {
                type.addMethod(MethodSpec.methodBuilder(m.adder())
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(self)
                        .addParameter(elementType(m.property().type()), "item")
                        .addStatement("super.$N(item)", m.adder())
                        .addStatement("return this")
                        .build());
            }
        }

        /**
         * Sets the back-reference on {@code child} (of an owned end) to this object. Skipped when this
         * class is not a Java subtype of the back-reference's type, which happens only for an
         * association inherited through a flattened supertype.
         */
        private CodeBlock backLink(ClassDef self, Member m, String child) {
            PropertyDef opposite = m.end().opposite();
            if (!isJavaSubtype(self.qualifiedName(), opposite.type().name())) {
                return CodeBlock.of("");
            }
            if (opposite.multiplicity().isToMany()) {
                String getter = JavaNames.getter(opposite.name());
                return CodeBlock.builder()
                        .beginControlFlow("if ($L.$N().stream().noneMatch(e -> e == this))", child, getter)
                        .addStatement("$L.$N().add(this)", child, getter)
                        .endControlFlow()
                        .build();
            }
            return CodeBlock.of("$L.$N(this);\n", child, JavaNames.setter(opposite.name()));
        }

        private MethodSpec relink(ClassDef c, List<Member> own, boolean hasSuper) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("relink")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC);
            if (hasSuper) {
                method.addStatement("super.relink()");
            }
            for (Member m : own) {
                if (!m.serialized() || !model.isClass(m.property().type())) {
                    continue;
                }
                if (m.kind() == Kind.OWNED) {
                    CodeBlock link = backLink(c, m, "child");
                    if (!link.isEmpty()) {
                        if (m.list()) {
                            method.beginControlFlow("if (this.$N != null)", m.field())
                                    .beginControlFlow("for ($T child : this.$N)", elementType(m.property().type()), m.field())
                                    .beginControlFlow("if (child != null)")
                                    .addCode(link)
                                    .endControlFlow()
                                    .endControlFlow()
                                    .endControlFlow();
                        } else {
                            method.beginControlFlow("if (this.$N != null)", m.field())
                                    .addCode(backLink(c, m, "this." + m.field()))
                                    .endControlFlow();
                        }
                    }
                }
                method.addStatement("$T.$N(this.$N)", ModelSupport.class, m.list() ? "relinkEach" : "relink", m.field());
            }
            return method.build();
        }

        private MethodSpec validation(List<Member> own, boolean hasSuper) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("collectValidationErrors")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(String.class, "path")
                    .addParameter(ParameterizedTypeName.get(LIST, ClassName.get(String.class)), "errors");
            if (hasSuper) {
                method.addStatement("super.collectValidationErrors(path, errors)");
            }
            for (Member m : own) {
                if (!m.serialized()) {
                    continue;
                }
                var mult = m.property().multiplicity();
                String path = "." + m.name();
                if (m.list()) {
                    method.addStatement("$T.size(this.$N, $L, $L, path + $S, $S, errors)", ModelSupport.class, m.field(),
                            mult.lower(), mult.upper() == null ? "null" : mult.upper().toString(), path, mult.toString());
                } else if (mult.isRequired()) {
                    method.addStatement("$T.required(this.$N, path + $S, $S, errors)", ModelSupport.class, m.field(), path,
                            mult.toString());
                }
                if (model.isClass(m.property().type())) {
                    method.addStatement("$T.$N(this.$N, path + $S, errors)", ModelSupport.class,
                            m.list() ? "validateEach" : "validate", m.field(), path);
                }
            }
            return method.build();
        }

        private static MethodSpec equalsMethod(ClassName self, List<Member> own, boolean hasSuper) {
            MethodSpec.Builder method = MethodSpec.methodBuilder("equals")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(boolean.class)
                    .addParameter(Object.class, "o")
                    .beginControlFlow("if (this == o)")
                    .addStatement("return true")
                    .endControlFlow()
                    .beginControlFlow("if (o == null || getClass() != o.getClass())")
                    .addStatement("return false")
                    .endControlFlow();
            List<Member> fields = own.stream().filter(Member::serialized).toList();
            if (hasSuper) {
                if (fields.isEmpty()) {
                    return method.addStatement("return super.equals(o)").build();
                }
                method.beginControlFlow("if (!super.equals(o))").addStatement("return false").endControlFlow();
            }
            if (fields.isEmpty()) {
                return method.addStatement("return true").build();
            }
            method.addStatement("$T that = ($T) o", self, self);
            CodeBlock comparison = CodeBlock.join(fields.stream()
                    .map(m -> CodeBlock.of("$T.equals(this.$N, that.$N)", Objects.class, m.field(), m.field()))
                    .toList(), "\n&& ");
            return method.addStatement("return $L", comparison).build();
        }

        private static MethodSpec hashCodeMethod(List<Member> own, boolean hasSuper) {
            List<CodeBlock> parts = new ArrayList<>();
            if (hasSuper) {
                parts.add(CodeBlock.of("super.hashCode()"));
            }
            own.stream().filter(Member::serialized).forEach(m -> parts.add(CodeBlock.of("this.$N", m.field())));
            return MethodSpec.methodBuilder("hashCode")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(int.class)
                    .addStatement("return $T.hash($L)", Objects.class, CodeBlock.join(parts, ", "))
                    .build();
        }

        // ---- enum ----

        private TypeSpec enumType(EnumDef e) {
            TypeSpec.Builder type = TypeSpec.enumBuilder(typeNames.get(e.qualifiedName()))
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(generated())
                    .addJavadoc("Generated from Pure enumeration <code>$L</code> ($L).\n",
                            html(e.qualifiedName()), html(e.location().toString()))
                    .addAnnotations(pureAnnotations(e.stereotypes(), e.tags()));
            for (EnumValueDef v : e.values()) {
                String constant = JavaNames.identifier(v.name());
                List<AnnotationSpec> annotations = new ArrayList<>();
                if (!constant.equals(v.name())) {
                    annotations.add(AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", v.name()).build());
                }
                annotations.addAll(pureAnnotations(v.stereotypes(), v.tags()));
                if (annotations.isEmpty()) {
                    type.addEnumConstant(constant);
                } else {
                    type.addEnumConstant(constant, TypeSpec.anonymousClassBuilder("").addAnnotations(annotations).build());
                }
            }
            return type.build();
        }

        // ---- per-file model class ----

        private TypeSpec modelType(String file, List<String> elements) {
            String name = JavaNames.fileStem(file) + "Model";
            TypeName classList = ParameterizedTypeName.get(LIST,
                    ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)));
            CodeBlock classes = CodeBlock.join(
                    elements.stream().map(q -> CodeBlock.of("$T.class", typeNames.get(q))).toList(), ",\n");
            return TypeSpec.classBuilder(name)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addAnnotation(generated())
                    .addJavadoc("Types generated from <code>$L</code>.\n", html(file))
                    .addField(FieldSpec.builder(String.class, "MERMAID", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                            .addJavadoc("Mermaid class diagram of the model; owned association ends are drawn as composition.\n")
                            .initializer("$S", mermaid(file, elements))
                            .build())
                    .addField(FieldSpec.builder(classList, "CLASSES", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                            .addJavadoc("Generated classes and enums, in model order.\n")
                            .initializer("$T.of(\n$>$L$<)", List.class, classes)
                            .build())
                    .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build())
                    .build();
        }

        String mermaid(String file, List<String> elements) {
            StringBuilder sb = new StringBuilder("classDiagram\n");
            for (String q : elements) {
                String id = mermaidIds.get(q);
                if (model.findEnum(q).isPresent()) {
                    sb.append("  class ").append(id).append(" {\n    <<enumeration>>\n");
                    model.findEnum(q).orElseThrow().values().forEach(v -> sb.append("    ").append(v.name()).append('\n'));
                    sb.append("  }\n");
                    continue;
                }
                ClassDef c = cls(q);
                sb.append("  class ").append(id).append(" {\n");
                for (PropertyDef p : c.properties()) {
                    sb.append("    +").append(p.name()).append(" : ").append(mermaidType(p.type())).append(p.multiplicity()).append('\n');
                }
                for (DerivedPropertyDef d : c.derived()) {
                    sb.append("    +").append(d.name()).append("() ").append(mermaidType(d.returnType())).append(d.multiplicity())
                            .append('\n');
                }
                sb.append("  }\n");
            }
            for (String q : elements) {
                model.findClass(q).ifPresent(c -> c.superTypes().forEach(s -> sb.append("  ").append(mermaidIds.get(s))
                        .append(" <|-- ").append(mermaidIds.get(q)).append('\n')));
            }
            for (AssociationDef a : model.associations()) {
                if (!a.location().file().equals(file)) {
                    continue;
                }
                // end1 is a property of end2's class; find which end is owned from that class's point of view.
                ClassDef holderOfEnd2 = cls(a.end1().type().name());
                boolean end2Owned = model.associationProperties(holderOfEnd2).stream()
                        .anyMatch(e -> e.association().equals(a) && e.navigable().equals(a.end2()) && e.owned());
                boolean end1Owned = model.associationProperties(cls(a.end2().type().name())).stream()
                        .anyMatch(e -> e.association().equals(a) && e.navigable().equals(a.end1()) && e.owned());
                if (end2Owned || end1Owned) {
                    PropertyDef owned = end2Owned ? a.end2() : a.end1();
                    PropertyDef back = end2Owned ? a.end1() : a.end2();
                    sb.append("  ").append(mermaidIds.get(back.type().name())).append(" \"").append(bounds(back)).append("\" *-- \"")
                            .append(bounds(owned)).append("\" ").append(mermaidIds.get(owned.type().name())).append(" : ")
                            .append(owned.name()).append('\n');
                } else {
                    sb.append("  ").append(mermaidIds.get(a.end1().type().name())).append(" \"").append(bounds(a.end1()))
                            .append("\" -- \"").append(bounds(a.end2())).append("\" ").append(mermaidIds.get(a.end2().type().name()))
                            .append(" : ").append(a.end1().name()).append(" / ").append(a.end2().name()).append('\n');
                }
            }
            return sb.toString();
        }

        private String mermaidType(TypeRef type) {
            return type.primitive() ? type.name() : mermaidIds.getOrDefault(type.name(), simpleName(type.name()));
        }

        private static String bounds(PropertyDef p) {
            String m = p.multiplicity().toString();
            return m.substring(1, m.length() - 1);
        }

        // ---- annotations and docs ----

        private static AnnotationSpec generated() {
            return AnnotationSpec.builder(GENERATED).addMember("value", "$S", GENERATOR).build();
        }

        private static List<AnnotationSpec> pureAnnotations(List<StereotypeRef> stereotypes, List<TaggedValue> tags) {
            List<AnnotationSpec> result = new ArrayList<>();
            for (StereotypeRef s : stereotypes) {
                result.add(AnnotationSpec.builder(PureStereotype.class)
                        .addMember("value", "$S", s.profile() + "." + s.value())
                        .build());
            }
            for (TaggedValue t : tags) {
                result.add(AnnotationSpec.builder(PureTag.class)
                        .addMember("profile", "$S", t.profile())
                        .addMember("tag", "$S", t.tag())
                        .addMember("value", "$S", t.value())
                        .build());
            }
            return result;
        }

        /**
         * Escapes model text for a Javadoc comment: HTML specials, {@code @} (inline tags), backslashes
         * (unicode escapes are processed even in comments) and comment terminators; whitespace runs
         * collapse so multi-line expressions stay on one line.
         */
        private static String html(String text) {
            StringBuilder sb = new StringBuilder();
            for (char ch : text.replaceAll("\\s+", " ").toCharArray()) {
                switch (ch) {
                    case '&' -> sb.append("&amp;");
                    case '<' -> sb.append("&lt;");
                    case '>' -> sb.append("&gt;");
                    case '@' -> sb.append("&#64;");
                    case '\\' -> sb.append("&#92;");
                    default -> sb.append(ch);
                }
            }
            return sb.toString().replace("*/", "*&#47;");
        }
    }
}
