package io.concert.model.pure;

final class QualifiedNames {

    private QualifiedNames() {}

    static String packageOf(String qualifiedName) {
        int i = qualifiedName.lastIndexOf("::");
        return i < 0 ? "" : qualifiedName.substring(0, i);
    }

    static String simpleName(String qualifiedName) {
        int i = qualifiedName.lastIndexOf("::");
        return i < 0 ? qualifiedName : qualifiedName.substring(i + 2);
    }

    static boolean isQualified(String name) {
        return name.contains("::");
    }
}
