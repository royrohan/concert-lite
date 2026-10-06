// The trading domain, defined only in Pure (src/main/pure). Everything else -- the concert state
// machines and generator (trading), the market data simulator (marketdata-sim) and the analytics
// sinks -- codes against the classes generated here, never against hand-written model types.
dependencies {
    api(project(":model-runtime"))
    annotationProcessor(project(":model-codegen"))
}
