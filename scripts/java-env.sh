# Sourced by the ecosystem scripts: makes JAVA_HOME point at a JDK 25 (Gradle and the generated tools need it)
# when the current one is older. Override by exporting JAVA_HOME yourself.
concert_java_ok() { [ -n "${1:-}" ] && [ -x "$1/bin/java" ] && "$1/bin/java" -version 2>&1 | grep -Eq 'version "(2[5-9]|[3-9][0-9])'; }
if ! concert_java_ok "${JAVA_HOME:-}"; then
  for candidate in /opt/homebrew/opt/openjdk@25 /usr/local/opt/openjdk@25 "$(/usr/libexec/java_home -v 25 2>/dev/null || true)"; do
    if concert_java_ok "$candidate"; then
      export JAVA_HOME="$candidate"
      break
    fi
  done
fi
if ! concert_java_ok "${JAVA_HOME:-}"; then
  echo "warning: no JDK 25 found (set JAVA_HOME); Gradle's toolchain may still work" >&2
fi
