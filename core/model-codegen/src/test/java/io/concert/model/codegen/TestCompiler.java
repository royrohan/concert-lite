package io.concert.model.codegen;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.Processor;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

/** Compiles sources on disk with the system compiler against the test runtime classpath. */
final class TestCompiler {

    record Result(boolean success, List<Diagnostic<? extends JavaFileObject>> diagnostics, Path classes, Path generated) {

        List<String> messages(Diagnostic.Kind kind) {
            return diagnostics.stream()
                    .filter(d -> d.getKind() == kind)
                    .map(d -> d.getMessage(Locale.ROOT))
                    .toList();
        }

        String report() {
            return diagnostics.stream().map(d -> d.getKind() + ": " + d).collect(Collectors.joining("\n"));
        }

        URLClassLoader classLoader() {
            try {
                return new URLClassLoader(new URL[] {classes.toUri().toURL()}, TestCompiler.class.getClassLoader());
            } catch (MalformedURLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private TestCompiler() {}

    /** The test runtime classpath minus entries that do not exist, which {@code -Xlint:path} would flag. */
    static String classpath() {
        return Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .filter(p -> Files.exists(Path.of(p)))
                .collect(Collectors.joining(File.pathSeparator));
    }

    static List<Path> javaSources(Path dir) {
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(p -> p.toString().endsWith(".java")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Result compile(Path work, List<Path> sources, List<String> options, List<Processor> processors) {
        try {
            Path classes = Files.createDirectories(work.resolve("classes"));
            Path generated = Files.createDirectories(work.resolve("generated"));
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
            List<String> args = new ArrayList<>(List.of(
                    "-classpath", classpath(), "-d", classes.toString(), "-s", generated.toString(), "-encoding", "UTF-8"));
            args.addAll(options);
            if (processors.isEmpty()) {
                args.add("-proc:none");
            }
            try (StandardJavaFileManager fm = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
                JavaCompiler.CompilationTask task =
                        compiler.getTask(null, fm, diagnostics, args, null, fm.getJavaFileObjectsFromPaths(sources));
                if (!processors.isEmpty()) {
                    task.setProcessors(processors);
                }
                boolean ok = task.call();
                return new Result(ok, diagnostics.getDiagnostics(), classes, generated);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
