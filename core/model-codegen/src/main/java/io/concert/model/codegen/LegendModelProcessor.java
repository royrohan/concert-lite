package io.concert.model.codegen;

import com.palantir.javapoet.JavaFile;
import com.palantir.javapoet.TypeSpec;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureModelException;
import io.concert.model.pure.PureParseException;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import io.concert.model.runtime.LegendModel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

/**
 * Generates Java classes for the Pure model named by each {@link LegendModel} handler. Model files
 * are looked up in the comma-separated directories of the {@value #MODEL_ROOTS} option (absolute, or
 * relative to the compiler's working directory).
 *
 * <p>A handler names one file ({@code file}) or a set of files ({@code files}) that are parsed and
 * resolved together. Each distinct file set is generated once per compilation even when several
 * handlers name it; every such handler is an originating element of the generated sources. A file may
 * belong to several sets (e.g. a shared {@code common.pure}); its types are then written once, and it
 * is an error if another set would generate them differently. Generating one file into two different
 * Java packages is an error. Model errors are reported as compiler errors on the {@code @LegendModel}
 * annotation, resolver warnings as notes so {@code -Werror} builds still pass.
 *
 * <p>Model files are not Java sources, so the build must declare them as compile task inputs for
 * changes to trigger recompilation.
 */
@SupportedAnnotationTypes("io.concert.model.runtime.LegendModel")
@SupportedOptions(LegendModelProcessor.MODEL_ROOTS)
public final class LegendModelProcessor extends AbstractProcessor {

    public static final String MODEL_ROOTS = "concert.modelRoots";

    /** Java package each model file was generated into, across processing rounds. */
    private final Map<Path, String> filePackages = new HashMap<>();

    /** File sets already generated, across processing rounds. */
    private final Set<List<Path>> generatedSets = new HashSet<>();

    /** Source of every written type and the file set that produced it, by qualified name. */
    private final Map<String, Written> written = new HashMap<>();

    private record Written(String source, String files) {}

    private record Handler(TypeElement type, AnnotationMirror annotation, LegendModel model, List<String> files, String basePackage) {}

    /** One model file: its location and its name as written in the annotation (used in messages). */
    private record ModelFile(Path path, String name) {}

    private record Request(List<ModelFile> files, String basePackage, List<Handler> handlers) {

        String describe() {
            return files.size() == 1 ? files.getFirst().name() : files.stream().map(ModelFile::name).toList().toString();
        }
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Map<List<Path>, Request> requests = new LinkedHashMap<>();
        Map<Path, String> pendingPackages = new HashMap<>();
        for (Element e : roundEnv.getElementsAnnotatedWith(LegendModel.class)) {
            if (!(e instanceof TypeElement type)) {
                continue;
            }
            Handler handler = handler(type);
            if (handler == null) {
                continue;
            }
            List<ModelFile> files = locate(handler);
            if (files == null) {
                continue;
            }
            String conflict = null;
            for (ModelFile f : files) {
                String previous = pendingPackages.getOrDefault(f.path(), filePackages.get(f.path()));
                if (previous != null && !previous.equals(handler.basePackage())) {
                    conflict = "model file " + f.name() + " is already generated into package " + previous
                            + "; every handler naming it must use the same javaPackage, found " + handler.basePackage();
                    break;
                }
            }
            if (conflict != null) {
                error(handler, conflict);
                continue;
            }
            files.forEach(f -> pendingPackages.put(f.path(), handler.basePackage()));
            List<Path> key = files.stream().map(ModelFile::path).sorted().toList();
            requests.computeIfAbsent(key, k -> new Request(files, handler.basePackage(), new ArrayList<>()))
                    .handlers().add(handler);
        }
        requests.forEach(this::generate);
        return true;
    }

    /** The handler's settings, or {@code null} after reporting an invalid file/files combination. */
    private Handler handler(TypeElement type) {
        LegendModel model = type.getAnnotation(LegendModel.class);
        AnnotationMirror mirror = type.getAnnotationMirrors().stream()
                .filter(m -> ((TypeElement) m.getAnnotationType().asElement()).getQualifiedName()
                        .contentEquals(LegendModel.class.getCanonicalName()))
                .findFirst()
                .orElse(null);
        String basePackage = model.javaPackage();
        if (basePackage.isEmpty()) {
            PackageElement pkg = processingEnv.getElementUtils().getPackageOf(type);
            basePackage = pkg.isUnnamed() ? "model" : pkg.getQualifiedName() + ".model";
        }
        boolean hasFile = !model.file().isBlank();
        List<String> files = Arrays.stream(model.files()).map(String::strip).filter(f -> !f.isEmpty()).distinct().toList();
        Handler handler = new Handler(type, mirror, model, hasFile ? List.of(model.file().strip()) : files, basePackage);
        if (hasFile == !files.isEmpty()) {
            error(handler, "@LegendModel must set exactly one of file and files" + (hasFile ? ", not both" : ""));
            return null;
        }
        return handler;
    }

    /** The model files of {@code handler}, or {@code null} after reporting which were not found. */
    private List<ModelFile> locate(Handler handler) {
        String roots = processingEnv.getOptions().get(MODEL_ROOTS);
        if (roots == null || roots.isBlank()) {
            error(handler, "option -A" + MODEL_ROOTS + " is not set; it must list the directories containing Pure models");
            return null;
        }
        List<Path> searched = new ArrayList<>();
        for (String root : roots.split(",")) {
            if (!root.isBlank()) {
                searched.add(Path.of(root.strip()).toAbsolutePath().normalize());
            }
        }
        List<ModelFile> found = new ArrayList<>();
        boolean ok = true;
        for (String file : handler.files()) {
            Path path = searched.stream()
                    .map(dir -> dir.resolve(file).normalize())
                    .filter(Files::isRegularFile)
                    .findFirst()
                    .orElse(null);
            if (path == null) {
                error(handler, "model file " + file + " not found in " + MODEL_ROOTS + " directories: "
                        + String.join(", ", searched.stream().map(Path::toString).toList()));
                ok = false;
            } else {
                found.add(new ModelFile(path, file));
            }
        }
        return ok ? found : null;
    }

    private void generate(List<Path> key, Request request) {
        if (!generatedSets.add(key)) {
            return;
        }
        request.files().forEach(f -> filePackages.put(f.path(), request.basePackage()));
        ResolvedModel model;
        try {
            List<PureModel> parsed = new ArrayList<>();
            for (ModelFile f : request.files()) {
                parsed.add(PureParser.parse(Files.readString(f.path(), StandardCharsets.UTF_8), f.name()));
            }
            model = new ModelResolver().resolve(parsed.toArray(PureModel[]::new));
        } catch (IOException e) {
            request.handlers().forEach(h -> error(h, "cannot read model file: " + e.getMessage()));
            return;
        } catch (PureParseException e) {
            request.handlers().forEach(h -> error(h, e.getMessage()));
            return;
        } catch (PureModelException e) {
            request.handlers().forEach(h -> e.errors().forEach(msg -> error(h, msg)));
            return;
        }
        boolean rootsOk = true;
        for (Handler h : request.handlers()) {
            if (model.findClass(h.model().root()).isEmpty()) {
                error(h, "root " + h.model().root() + " is not a class in " + request.describe()
                        + (model.classes().isEmpty() ? "" : "; classes: " + String.join(", ",
                                model.classes().stream().map(ClassDef::qualifiedName).toList())));
                rootsOk = false;
            }
        }
        if (!rootsOk) {
            return;
        }
        for (String warning : model.warnings()) {
            Handler first = request.handlers().getFirst();
            processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, warning, first.type(), first.annotation());
        }
        for (JavaFile file : new JavaGenerator().generate(model, request.basePackage())) {
            TypeSpec.Builder type = file.typeSpec().toBuilder();
            request.handlers().forEach(h -> type.addOriginatingElement(h.type()));
            JavaFile out = JavaGenerator.javaFile(file.packageName(), type.build());
            String qualifiedName = file.packageName() + "." + file.typeSpec().name();
            String source = out.toString();
            Written previous = written.get(qualifiedName);
            if (previous != null) {
                if (!previous.source().equals(source)) {
                    request.handlers().forEach(h -> error(h, qualifiedName + " is generated differently from model files "
                            + request.describe() + " than from " + previous.files() + "; classes of a shared model file must not"
                            + " be extended or associated by the other files of a set"));
                }
                continue;
            }
            written.put(qualifiedName, new Written(source, request.describe()));
            try {
                out.writeTo(processingEnv.getFiler());
            } catch (IOException e) {
                request.handlers().forEach(h -> error(h, "cannot write " + qualifiedName + ": " + e.getMessage()));
                return;
            }
        }
    }

    private void error(Handler handler, String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, handler.type(), handler.annotation());
    }
}
