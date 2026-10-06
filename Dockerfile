# syntax=docker/dockerfile:1.7
# One build, several runtime images (targets): coordinator, worker, trace-ui, sink-duckdb, sink-clickhouse,
# marketdata-sim, trading-worker, trading-loadgen, and showcase-worker (any generated ecosystem, by build args).
#   docker compose build            (compose picks the target per service)

FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon -q --version
COPY . .
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon -q :orchestration:installDist :sample-workers:installDist :trace-ui:installDist \
      :sink-duckdb:installDist :sink-clickhouse:installDist :marketdata-sim:installDist \
      :trading:installDist

FROM eclipse-temurin:25-jre AS runtime
RUN useradd --system --uid 10001 concert
USER concert
ENV JAVA_OPTS="-XX:+UseZGC -XX:+UseCompactObjectHeaders -XX:MaxRAMPercentage=75"

FROM runtime AS coordinator
COPY --from=build /src/platform/orchestration/build/install/orchestration /app
ENTRYPOINT ["/app/bin/orchestration"]

FROM runtime AS worker
COPY --from=build /src/showcases/sample-workers/build/install/sample-workers /app
ENTRYPOINT ["/app/bin/sample-workers"]

FROM runtime AS trace-ui
COPY --from=build /src/platform/trace-ui/build/install/trace-ui /app
EXPOSE 8088
ENTRYPOINT ["/app/bin/trace-ui"]

FROM runtime AS sink-duckdb
COPY --from=build /src/analytics/sink-duckdb/build/install/sink-duckdb /app
# The DuckDB file lives on a volume; create the mount point owned by the runtime user.
USER root
RUN mkdir -p /data && chown concert /data
USER concert
EXPOSE 8090
ENTRYPOINT ["/app/bin/sink-duckdb"]

# One-shot: generates and applies the ClickHouse schema (Kafka engine tables, views), then exits.
FROM runtime AS sink-clickhouse
COPY --from=build /src/analytics/sink-clickhouse/build/install/sink-clickhouse /app
ENV JAVA_OPTS="-XX:+UseSerialGC -XX:MaxRAMPercentage=75"
ENTRYPOINT ["/app/bin/sink-clickhouse"]

# Market data straight to Kafka (ref.*, md.ticks, md.bars.1m), bypassing concert.
FROM runtime AS marketdata-sim
COPY --from=build /src/analytics/marketdata-sim/build/install/marketdata-sim /app
ENTRYPOINT ["/app/bin/marketdata-sim"]

# Trading showcase: the four trading state machine workers (bin/trading-worker) ...
FROM runtime AS trading-worker
COPY --from=build /src/showcases/trading/build/install/trading /app
ENTRYPOINT ["/app/bin/trading-worker"]

# ... and the one-shot order-flow generator (bin/trading, TradingLoadGen; ORDERS, RATE, SEED, VOL_MULT env).
FROM runtime AS trading-loadgen
COPY --from=build /src/showcases/trading/build/install/trading /app
ENTRYPOINT ["/app/bin/trading"]

# Generated ecosystems (./generate-concert-ecosystem): one generic worker image per module, selected with build
# args, e.g. in showcases/insurance/compose.yml:
#   build: { context: ., target: showcase-worker, args: { MODULE: insurance, MODULE_DIR: showcases/insurance } }
FROM build AS showcase-build
ARG MODULE
ARG MODULE_DIR
RUN --mount=type=cache,target=/root/.gradle \
    test -n "$MODULE" && test -n "$MODULE_DIR" \
    && ./gradlew --no-daemon -q ":${MODULE}:installDist" \
    && mkdir -p /out && cp -r "${MODULE_DIR}/build/install/${MODULE}" /out/app

FROM runtime AS showcase-worker
COPY --from=showcase-build /out/app /app
ENTRYPOINT ["/app/bin/ecosystem-worker"]
