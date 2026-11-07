# legend-lite (https://github.com/neema2/legend-lite) pinned to one commit and built from source with Bazel: it has no
# releases or published artifact and its API moves daily, so concert builds exactly the commit it was tested against.
# The dashboard's Model query tab uses it only to plan (Pure -> SQL); concert runs the SQL on DuckDB / ClickHouse itself.
# Two images from one build: target "server" (legend-lite's HTTP API) and target "studio" (Legend Studio Lite, level 0:
# a static site whose compiler and SDLC run in the browser as WebAssembly; its demo projects/ are concert's models,
# proxied from the dashboard).
#
#   docker compose --profile analytics build legend-lite legend-studio
#   LEGEND_LITE_REF=<commit> docker compose --profile analytics build legend-lite   # move the pin (re-run the checks)
#
FROM eclipse-temurin:25-jdk AS build
ARG LEGEND_LITE_REF=42b43f3cc82d478fc2f70d18682407b27e978efa
ARG BAZELISK_VERSION=v1.27.0
RUN apt-get update && apt-get install -y --no-install-recommends git build-essential python3 unzip zip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN arch=$(dpkg --print-architecture) \
    && curl -fsSL -o /usr/local/bin/bazel "https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-${arch}" \
    && chmod +x /usr/local/bin/bazel
# Bazel's Python rules refuse to run as root.
RUN useradd -m -u 1001 builder && mkdir -p /src /out && chown builder /src /out
USER builder
WORKDIR /src
RUN git init -q legend-lite && cd legend-lite \
    && git remote add origin https://github.com/neema2/legend-lite.git \
    && git fetch -q --depth 1 origin "${LEGEND_LITE_REF}" && git checkout -q FETCH_HEAD
WORKDIR /src/legend-lite
RUN --mount=type=cache,target=/home/builder/.cache,uid=1001 \
    bazel build //core:server_deploy.jar //studio:serve \
    && cp bazel-bin/core/server_deploy.jar /out/legend-lite.jar \
    && mkdir -p /out/studio \
    && cp -rL bazel-bin/studio/serve_/serve.runfiles/_main/studio/demo bazel-bin/studio/serve_/serve.runfiles/_main/studio/src /out/studio/ \
    && rm -rf /out/studio/demo/projects/* /out/studio/demo/serve.mjs \
    && git rev-parse HEAD > /out/COMMIT

# Legend Studio Lite (static site on nginx, port 8200)
FROM nginx:1.29-alpine AS studio
COPY --from=build /out/studio/ /usr/share/nginx/html/
COPY --from=build /out/COMMIT /usr/share/nginx/html/COMMIT
COPY studio-nginx.conf /etc/nginx/conf.d/default.conf
# concert's add-on: ?concert=<projectId> publishes (first time) and opens concert's project
COPY concert-open.js /usr/share/nginx/html/demo/concert-open.js
RUN sed -i 's#<script type="module" src="./bundle.js"></script>#&\n  <script src="./concert-open.js" defer></script>#' /usr/share/nginx/html/demo/index.html \
    && grep -q concert-open.js /usr/share/nginx/html/demo/index.html
EXPOSE 8200

# legend-lite's HTTP API (port 8095)
FROM eclipse-temurin:25-jre AS server
COPY --from=build /out/ /app/
ENV PORT=8095
EXPOSE 8095
HEALTHCHECK --interval=5s --timeout=3s --retries=30 CMD ["sh", "-c", "exec 3<>/dev/tcp/127.0.0.1/8095 && printf 'GET /health HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q ok <&3"]
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/legend-lite.jar"]
